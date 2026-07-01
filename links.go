package main

import (
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strings"
)

func LoadLinks(path string, fallback []string) ([]string, error) {
	if strings.TrimSpace(path) == "" {
		if len(fallback) == 0 {
			return []string{}, nil
		}
		return normalizeLinks(fallback)
	}

	content, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	trimmed := strings.TrimSpace(string(content))
	if trimmed == "" {
		return nil, fmt.Errorf("URL file is empty")
	}

	var links []string
	if strings.HasPrefix(trimmed, "[") {
		if err := json.Unmarshal(content, &links); err != nil {
			return nil, err
		}
	} else {
		for _, line := range strings.Split(trimmed, "\n") {
			line = strings.TrimSpace(line)
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			links = append(links, line)
		}
	}

	return normalizeLinks(links)
}

func LoadSeedLinks(cfg Config) ([]string, error) {
	var fallback []string
	if cfg.SeedDefaultLinks {
		fallback = DefaultLinks()
	}
	return LoadLinks(cfg.SeedURLsFile, fallback)
}

func ValidateLinks(links []string, policy *NetworkPolicy) error {
	if len(links) == 0 {
		return fmt.Errorf("at least one link is required")
	}

	for _, link := range links {
		if err := policy.ValidateURL(link); err != nil {
			return fmt.Errorf("%s: %w", link, err)
		}
	}
	return nil
}

func normalizeLinks(links []string) ([]string, error) {
	seen := make(map[string]struct{}, len(links))
	normalized := make([]string, 0, len(links))
	for _, link := range links {
		link = strings.TrimSpace(link)
		if link == "" {
			continue
		}
		if _, exists := seen[link]; exists {
			continue
		}
		seen[link] = struct{}{}
		normalized = append(normalized, link)
	}
	if len(normalized) == 0 {
		return nil, fmt.Errorf("at least one link is required")
	}
	slices.Sort(normalized)
	return normalized, nil
}

func DefaultLinks() []string {
	return []string{
		"https://4book.org",
		"https://a-level.com.ua",
		"https://abiturients.info",
		"https://antischool.online",
		"https://bazilik.media",
		"https://beetroot.academy",
		"https://besmart.edu.gethomestudy.com",
		"https://buki.com.ua",
		"https://cambridge.ua",
		"https://casers.org",
		"https://childcamp.com.ua",
		"https://choice31.com",
		"https://cursor.education",
		"https://dan-it.com.ua",
		"https://dnipro.it",
		"https://dnu.dp.ua",
		"https://donnu.edu.ua",
		"https://dou.ua",
		"https://edu.cbsystematics.com",
		"https://englisher.com.ua",
		"https://erudyt.net",
		"https://foxminded.ua",
		"https://gdz4you.com",
		"https://genius.space",
		"https://gioschool.com",
		"https://goit.global/ua",
		"https://grade.ua",
		"https://greenforest.com.ua",
		"https://greencountry.com.ua",
		"https://happymonday.ua",
		"https://iampm.club",
		"https://ilearn.org.ua",
		"https://impactorium.org",
		"https://info.edbo.gov.ua",
		"https://ithillel.ua",
		"https://itstep.org",
		"https://itvdn.com",
		"https://k-a-m-a.com",
		"https://khnu.km.ua",
		"https://kneu.edu.ua",
		"https://knutd.edu.ua",
		"https://kpi.ua",
		"https://kursi.ua",
		"https://l-a-b-a.com",
		"https://learning.ua",
		"https://lemon.school",
		"https://lms.e-school.net.ua",
		"https://lnu.edu.ua",
		"https://lpnu.edu.ua",
		"https://mainacademy.com",
		"https://man.gov.ua",
		"https://mate.academy",
		"https://miyklas.com.ua",
		"https://mon.gov.ua",
		"https://nau.edu.ua",
		"https://nmu.org.ua",
		"https://nmuofficial.com",
		"https://nubip.edu.ua",
		"https://nure.ua",
		"https://nus.org.ua",
		"https://onu.edu.ua",
		"https://onpu.edu.ua",
		"https://osvita.diia.gov.ua",
		"https://osvita.ua",
		"https://parta.com.ua",
		"https://pkk.com.ua",
		"https://prjctr.com",
		"https://prog.kiev.ua",
		"https://prometheus.org.ua",
		"https://robotdreams.cc",
		"https://rozum.com",
		"https://shkola.in.ua",
		"https://sigma.software/university",
		"https://skillup.ua",
		"https://skvot.io",
		"https://speak-up.com.ua",
		"https://studway.com.ua",
		"https://subject.com.ua",
		"https://sumdu.edu.ua",
		"https://superludi.com",
		"https://testportal.gov.ua",
		"https://tntu.edu.ua",
		"https://ua.mozaweb.com",
		"https://ucu.edu.ua",
		"https://urok-ua.com",
		"https://uzhnu.edu.ua",
		"https://video.novashkola.ua",
		"https://vseosvita.ua",
		"https://vumonline.ua",
		"https://web-academy.com.ua",
		"https://wisecow.com.ua",
		"https://www.britishcouncil.org.ua",
		"https://www.chnu.edu.ua",
		"https://www.ed-era.com",
		"https://www.education.ua",
		"https://www.englishdom.com",
		"https://www.k-a-m-a.com",
		"https://www.mathema.me",
		"https://www.nmu.org.ua",
		"https://www.study.ua",
		"https://www.ukma.edu.ua",
		"https://www.univ.kiev.ua",
		"https://www.univer.kharkov.ua",
		"https://yappi.com.ua",
		"https://zno.ua",
		"https://znu.edu.ua",
		"http://www.nbuv.gov.ua",
	}
}
