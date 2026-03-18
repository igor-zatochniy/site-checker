package main

import (
	"fmt"
	"net/http"
	"time"
)

func main() {
	links := []string{
		"https://prometheus.org.ua",
		"https://www.ed-era.com",
		"https://osvita.diia.gov.ua",
		"https://vumonline.ua",
		"https://wisecow.com.ua",
		"https://impactorium.org",
		"https://prjctr.com",
		"https://mate.academy",
		"https://goit.global/ua",
		"https://ithillel.ua",
		"https://itstep.org",
		"https://beetroot.academy",
		"https://l-a-b-a.com",
		"https://robotdreams.cc",
		"https://skvot.io",
		"https://choice31.com",
		"https://iampm.club",
		"https://web-academy.com.ua",
		"https://edu.cbsystematics.com",
		"https://prog.kiev.ua",
		"https://skillup.ua",
		"https://mainacademy.com",
		"https://a-level.com.ua",
		"https://cursor.education",
		"https://foxminded.ua",
		"https://dan-it.com.ua",
		"https://lemon.school",
		"https://itvdn.com",
		"https://sigma.software/university",
		"https://www.k-a-m-a.com",
		"https://bazilik.media",
		"https://superludi.com",
		"https://casers.org",
		"https://happymonday.ua",
		"https://genius.space",
		"https://www.univ.kiev.ua",
		"https://kpi.ua",
		"https://www.ukma.edu.ua",
		"https://lpnu.edu.ua",
		"https://lnu.edu.ua",
		"https://www.univer.kharkov.ua",
		"https://sumdu.edu.ua",
		"https://nure.ua",
		"https://ucu.edu.ua",
		"https://www.nmu.org.ua",
		"https://onu.edu.ua",
		"https://www.chnu.edu.ua",
		"https://nau.edu.ua",
		"https://kneu.edu.ua",
		"https://nmuofficial.com",
		"https://nubip.edu.ua",
		"https://donnu.edu.ua",
		"https://uzhnu.edu.ua",
		"https://tntu.edu.ua",
		"https://khnu.km.ua",
		"https://znu.edu.ua",
		"https://dnu.dp.ua",
		"https://onpu.edu.ua",
		"https://knutd.edu.ua",
		"https://osvita.ua",
		"https://vseosvita.ua",
		"https://naurok.com.ua",
		"https://ilearn.org.ua",
		"https://gioschool.com",
		"https://ua.mozaweb.com",
		"https://nus.org.ua",
		"https://lms.e-school.net.ua",
		"https://rozum.com",
		"https://man.gov.ua",
		"https://www.mathema.me",
		"https://buki.com.ua",
		"https://zno.ua",
		"https://learning.ua",
		"https://miyklas.com.ua",
		"https://shkola.in.ua",
		"https://urok-ua.com",
		"https://erudyt.net",
		"https://subject.com.ua",
		"https://4book.org",
		"https://gdz4you.com",
		"https://greenforest.com.ua",
		"https://www.englishdom.com",
		"https://yappi.com.ua",
		"https://grade.ua",
		"https://speak-up.com.ua",
		"https://pkk.com.ua",
		"https://www.study.ua",
		"https://cambridge.ua",
		"https://www.britishcouncil.org.ua",
		"https://antischool.online",
		"https://greencountry.com.ua",
		"https://englisher.com.ua",
		"https://mon.gov.ua",
		"https://testportal.gov.ua",
		"https://info.edbo.gov.ua",
		"http://www.nbuv.gov.ua",
		"https://www.education.ua",
		"https://parta.com.ua",
		"https://dou.ua",
		"https://childcamp.com.ua",
		"https://kursi.ua",
		"https://studway.com.ua",
		"https://abiturients.info",
		"https://dnipro.it",
		"https://video.novashkola.ua",
		"https://besmart.edu.gethomestudy.com",
	}

	c := make(chan string)

	for _, link := range links {
		go checkLink(link, c)
	}

	for l := range c {
		go func(link string) {
			time.Sleep(5 * time.Second)
			checkLink(link, c)
		}(l)
	}
}

func checkLink(link string, c chan string) {
	_, err := http.Get(link)

	if err != nil {
		fmt.Println(link, "- isn't working ❌")
		c <- link
		return
	}

	fmt.Println(link, "- working now ✅")
	c <- link
}
