package main

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"
)

func main() {
	// Ініціалізуємо JSON-логер (стандарт для Docker-середовищ)
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	// Зчитуємо конфігурацію з ENV зі значеннями за замовчуванням (12-Factor App)
	workerCount := getEnvAsInt("WORKER_COUNT", 10)
	checkInterval := getEnvAsDuration("CHECK_INTERVAL", 5*time.Second)
	httpTimeout := getEnvAsDuration("HTTP_TIMEOUT", 3*time.Second)

	// Перелік сайтів для моніторингу
	links := []string{
		"https://prometheus.org.ua", "https://www.ed-era.com", "https://osvita.diia.gov.ua",
		"https://vumonline.ua", "https://wisecow.com.ua", "https://impactorium.org",
		"https://prjctr.com", "https://mate.academy", "https://goit.global/ua",
		"https://ithillel.ua", "https://itstep.org", "https://beetroot.academy",
		"https://l-a-b-a.com", "https://robotdreams.cc", "https://skvot.io",
		"https://choice31.com", "https://iampm.club", "https://web-academy.com.ua",
		"https://edu.cbsystematics.com", "https://prog.kiev.ua", "https://skillup.ua",
		"https://mainacademy.com", "https://a-level.com.ua", "https://cursor.education",
		"https://foxminded.ua", "https://dan-it.com.ua", "https://lemon.school",
		"https://itvdn.com", "https://sigma.software/university", "https://www.k-a-m-a.com",
		"https://bazilik.media", "https://superludi.com", "https://casers.org",
		"https://happymonday.ua", "https://genius.space", "https://www.univ.kiev.ua",
		"https://kpi.ua", "https://www.ukma.edu.ua", "https://lpnu.edu.ua",
		"https://lnu.edu.ua", "https://www.univer.kharkov.ua", "https://sumdu.edu.ua",
		"https://nure.ua", "https://ucu.edu.ua", "https://www.nmu.org.ua",
		"https://onu.edu.ua", "https://www.chnu.edu.ua", "https://nau.edu.ua",
		"https://kneu.edu.ua", "https://nmuofficial.com", "https://nubip.edu.ua",
		"https://donnu.edu.ua", "https://uzhnu.edu.ua", "https://tntu.edu.ua",
		"https://khnu.km.ua", "https://znu.edu.ua", "https://dnu.dp.ua",
		"https://onpu.edu.ua", "https://knutd.edu.ua", "https://osvita.ua",
		"https://vseosvita.ua", "https://naurok.com.ua", "https://ilearn.org.ua",
		"https://gioschool.com", "https://ua.mozaweb.com", "https://nus.org.ua",
		"https://lms.e-school.net.ua", "https://rozum.com", "https://man.gov.ua",
		"https://www.mathema.me", "https://buki.com.ua", "https://zno.ua",
		"https://learning.ua", "https://miyklas.com.ua", "https://shkola.in.ua",
		"https://urok-ua.com", "https://erudyt.net", "https://subject.com.ua",
		"https://4book.org", "https://gdz4you.com", "https://greenforest.com.ua",
		"https://www.englishdom.com", "https://yappi.com.ua", "https://grade.ua",
		"https://speak-up.com.ua", "https://pkk.com.ua", "https://www.study.ua",
		"https://cambridge.ua", "https://www.britishcouncil.org.ua", "https://antischool.online",
		"https://greencountry.com.ua", "https://englisher.com.ua", "https://mon.gov.ua",
		"https://testportal.gov.ua", "https://info.edbo.gov.ua", "http://www.nbuv.gov.ua",
		"https://www.education.ua", "https://parta.com.ua", "https://dou.ua",
		"https://childcamp.com.ua", "https://kursi.ua", "https://studway.com.ua",
		"https://abiturients.info", "https://dnipro.it", "https://video.novashkola.ua",
		"https://besmart.edu.gethomestudy.com",
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Налаштовуємо власний HTTP-транспорт для повторного використання TCP-з'єднань
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,             // Загальний ліміт вільних з'єднань у пулі
		MaxIdleConnsPerHost:   workerCount + 2, // Захист від повторного відкриття сокетів під час паралельних запитів до одного хоста
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}

	client := &http.Client{
		Transport: transport,
		Timeout:   httpTimeout,
	}

	linksChan := make(chan string, len(links))
	var wg sync.WaitGroup

	// Запускаємо пул воркерів із контролем через WaitGroup
	for i := 1; i <= workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			worker(ctx, linksChan, client)
		}()
	}

	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	slog.Info("Concurrent Site Checker started",
		"workers", workerCount,
		"interval", checkInterval,
		"timeout", httpTimeout,
	)

	for {
		select {
		case <-ctx.Done():
			slog.Info("Shutdown signal received. Stopping application...")
			close(linksChan) // Повідомляємо воркерам, що нових завдань не буде
			wg.Wait()        // Гарантовано очікуємо завершення активних мережевих запитів
			slog.Info("All workers completed gracefully. Application stopped.")
			return

		case <-ticker.C:
			slog.Info("Starting active checking wave", "total_links", len(links))

			for _, link := range links {
				select {
				case linksChan <- link:
					// Завдання успішно надіслано до буфера
				default:
					// Захист від перевантаження черги, якщо попередні запити зависли
					slog.Warn(
						"Worker pool is backed up. Skipping check to avoid flooding",
						"link",
						link,
					)
				}
			}
		}
	}
}

func worker(ctx context.Context, linksChan <-chan string, client *http.Client) {
	// Воркер читає канал, доки його не буде закрито (ok == false)
	for link := range linksChan {
		// Швидко перевіряємо скасування контексту перед тривалою мережевою операцією
		if ctx.Err() != nil {
			return
		}
		checkLink(ctx, link, client)
	}
}

func checkLink(ctx context.Context, link string, client *http.Client) {
	req, err := http.NewRequestWithContext(ctx, "GET", link, nil)
	if err != nil {
		slog.Error("Failed to create request", "link", link, "error", err)
		return
	}

	resp, err := client.Do(req)
	if err != nil {
		// Логуємо помилку, лише якщо контекст не було примусово завершено
		if ctx.Err() == nil {
			slog.Error("Site is down or unreachable", "link", link, "error", err.Error())
		}
		return
	}

	// Важливо: завжди закриваємо тіло відповіді та зчитуємо його до кінця (ігноруючи дані),
	// щоб Go міг повторно використати наявне TCP-з'єднання (Keep-Alive).
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}()

	if resp.StatusCode == http.StatusOK {
		slog.Info("Site is healthy", "link", link, "status", resp.StatusCode)
	} else {
		slog.Warn("Site returned non-200 status", "link", link, "status", resp.StatusCode)
	}
}

// Допоміжні функції для безпечного зчитування конфігурації з ENV
func getEnvAsInt(key string, defaultVal int) int {
	if valueStr, exists := os.LookupEnv(key); exists {
		if value, err := strconv.Atoi(valueStr); err == nil {
			return value
		}
	}
	return defaultVal
}

func getEnvAsDuration(key string, defaultVal time.Duration) time.Duration {
	if valueStr, exists := os.LookupEnv(key); exists {
		if value, err := time.ParseDuration(valueStr); err == nil {
			return value
		}
	}
	return defaultVal
}
