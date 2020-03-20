package main

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/PuerkitoBio/goquery"
)

var (
	domen        = "https://www.ozon.ru"
	fileURL      = "products_list.txt"
	fileCsv      = "products_data_w%d.csv"
	fileError    = "products_error_w%d.txt"
	fileStartURL = "catalog_list.txt"

	reportPeriod = 5
	workers      = 1
	step         = 2

	startSсanUrls = []string{}
)

// Product структура данных продукта
type Product struct {
	sku                 string
	name                string
	desc                string
	modeAppl            string
	composition         string
	indications         string
	length              string
	width               string
	height              string
	shippingWeight      string
	manufacturerCountry string
	brand               string
}

func getDocByURL(url string) (doc *goquery.Document, err error) {
	url = strings.TrimSpace(url)
	// Request the HTML page.
	res, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		return nil, fmt.Errorf("status code error: %d %s", res.StatusCode, res.Status)
		//return nil, errors.New(fmt.Sprintf("status code error: %d %s", res.StatusCode, res.Status))
		//log.Fatalf("status code error: %d %s", res.StatusCode, res.Status)
	}

	// Load the HTML document
	doc, err = goquery.NewDocumentFromReader(res.Body)
	if err != nil {
		return nil, err
		//log.Fatal(err)
	}
	// тормозимся
	//time.Sleep(100 * time.Millisecond)
	return doc, nil
}

func removeDuplicatesUnordered(elements []string) []string {
	encountered := map[string]bool{}

	// Create a map of all unique elements.
	for v := range elements {
		encountered[elements[v]] = true
	}

	// Place all keys from the map into a slice.
	result := []string{}
	for key := range encountered {
		result = append(result, key)
	}
	return result
}

func getProductList() {

	totalProductURLList := []string{}

	startSсanUrls, err := readLines(fileStartURL)
	check(err)
	fmt.Println(len(startSсanUrls))

	//os.Exit(1)

	for _, value := range startSсanUrls {

		// сделаем запрос на список товаров и количество страниц
		url := domen + value
		url = strings.TrimSpace(url)
		fmt.Println(url)

		productURLList, totalPages := requestToCategory(url, true)

		urlP := ""
		if totalPages > 1 {
			for i := 2; i < totalPages; i++ {
				urlP = url + "?page=" + strconv.Itoa(i)

				fmt.Println(urlP)
				productURLList2, _ := requestToCategory(urlP, false)

				if len(productURLList2) > 0 {
					productURLList = append(productURLList, productURLList2...)
				}

			}
		}
		fmt.Printf("len=%d cap=%d\n", len(productURLList), cap(productURLList))
		totalProductURLList = append(totalProductURLList, productURLList...)
		//os.Exit(1)
	}
	productURLListUnic := removeDuplicatesUnordered(totalProductURLList)
	sort.Strings(productURLListUnic)
	fmt.Printf("len=%d cap=%d \n", len(productURLListUnic), cap(productURLListUnic))
	saveProductListToFile(productURLListUnic)
}

// запрос по ссылки категории
func requestToCategory(url string, countTotalPages bool) (productURLList []string, totalPages int) {
	doc, err := getDocByURL(url)

	if err != nil {
		return []string{}, 0
	}

	//textHtml := doc.Find("*").Text()
	textHTML, _ := doc.Html()

	if countTotalPages {
		r, _ := regexp.Compile("\"totalPages\":([0-9]+)")
		matchPages := r.FindStringSubmatch(textHTML)

		totalPages, _ = strconv.Atoi(matchPages[1])
		fmt.Println("totalPages", totalPages)
	}
	rp, _ := regexp.Compile("/context/detail/id/[0-9]+/")
	productURLList = rp.FindAllString(textHTML, -1)
	return
}

func saveProductListToFile(productList []string) {

	f, err := os.OpenFile(fileURL, os.O_CREATE|os.O_WRONLY, 0644)

	check(err)
	defer f.Close()

	datawriter := bufio.NewWriter(f)
	for _, data := range productList {
		_, _ = datawriter.WriteString(data + "\n")
	}
	datawriter.Flush()
}

func saveErrorProductToFile(product string, w int) {
	file := fmt.Sprintf(fileError, w)
	f, err := os.OpenFile(file, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	check(err)
	defer f.Close()

	datawriter := bufio.NewWriter(f)
	_, _ = datawriter.WriteString(product + "\n")
	datawriter.Flush()
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func requestToDetail(url string) (p Product, err error) {
	p = Product{}

	r, _ := regexp.Compile("([0-9]+)")
	p.sku = r.FindString(url)

	url = strings.TrimSpace(url)

	doc, err := getDocByURL(url)
	if err != nil {
		return p, err
	}
	p.name = clearText(doc.Find("h1").Text())

	if len(p.name) <= 0 {
		return p, errors.New("Name is empty")
	}

	//textHTML, _ := doc.Html()
	//fmt.Println("textHTML", textHTML)

	p.desc = clearText(doc.Find("div#section-description > div").Text())

	//dataCharacteristics := []byte(doc.Find("script#state-characteristics-293080-default-1").Text())

	//var character interface{}
	//json.Unmarshal(dataCharacteristics, &character)
	//fmt.Printf("unpacked in empty interface:\n%#v\n\n", character)
	//os.Exit(1)

	fmt.Println(doc.Find("script#state-characteristics-293080-default-1").Text())
	fmt.Println("p.desc", p.desc)

	doc.Find("div#section-characteristics  dl").Each(func(i int, s *goquery.Selection) {
		propName := s.Find("dt").Text()
		propBody := s.Find("dd").Text()

		propName = clearText(propName)
		propBody = clearText(propBody)

		fmt.Println("propName", propName)
		fmt.Println("propBody", propBody)

		if strings.Index(propName, "Способ применения") == 0 {
			p.modeAppl = propBody
		} else if strings.Index(propName, "Показания") == 0 {
			p.indications = propBody
		} else if strings.Index(propName, "Состав") == 0 {
			p.composition = propBody
		} else if strings.Index(propName, "Размер упаковки") == 0 {

			r, _ := regexp.Compile("([0-9]+).*([0-9]+).*([0-9]+)")
			matchSize := r.FindStringSubmatch(propBody)

			for i, sizeItem := range matchSize {
				if i == 1 {
					p.length = sizeItem
				} else if i == 2 {
					p.width = sizeItem
				} else if i == 3 {
					p.height = sizeItem
				}
			}

			fmt.Println(matchSize)

		} else if strings.Index(propName, "Вес в упаковке") == 0 {
			p.shippingWeight = propBody
		} else if strings.Index(propName, "Страна-изготовитель") == 0 {
			p.manufacturerCountry = propBody
		} else if strings.Index(propName, "Бренд") == 0 {
			p.brand = propBody
		}

		os.Exit(1)

	})
	return
}

// получение информации с карточки товара
func getProductData() {

	lines, err := readLines(fileURL)
	check(err)
	fmt.Println(len(lines))

	productSlice := [][]string{{
		"sku",
		"name",
		"desc",
		"modeAppl",
		"composition",
		"indications",
		"length",
		"width",
		"height",
		"shippingWeight",
		"manufacturerCountry",
		"brand",
	}}
	saveProductCsv(productSlice, fileCsv)

	for _, detailURL := range lines {
		detailURL = strings.TrimSpace(detailURL)
		fmt.Println(detailURL)

		url := domen + detailURL
		p, err := requestToDetail(url)

		if err != nil {
			continue
		}

		pDataForCsv := [][]string{{
			p.sku,
			p.name,
			p.desc,
			p.modeAppl,
			p.composition,
			p.indications,
			p.length,
			p.width,
			p.height,
			p.shippingWeight,
			p.manufacturerCountry,
			p.brand,
		}}
		saveProductCsv(pDataForCsv, fileCsv)
		//productSlice = append(productSlice, pDataForCsv...)

	}
	//fmt.Printf("len=%d cap=%d\n", len(productSlice), cap(productSlice))
	//saveProductCsv(productSlice)
	os.Exit(1)

}

func saveProductCsv(productSlice [][]string, fileCsv string) {

	f, err := os.OpenFile(fileCsv, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	check(err)
	defer f.Close()
	w := csv.NewWriter(f)

	for _, value := range productSlice {
		w.Write(value)
	}
	w.Flush()
}

func readLines(filename string) ([]string, error) {
	var lines []string
	file, err := ioutil.ReadFile(filename)
	if err != nil {
		return lines, err
	}
	buf := bytes.NewBuffer(file)
	for {
		line, err := buf.ReadString('\n')
		if len(line) == 0 {
			if err != nil {
				if err == io.EOF {
					break
				}
				return lines, err
			}
		}
		lines = append(lines, line)
		if err != nil && err != io.EOF {
			return lines, err
		}
	}
	return lines, nil
}

func clearText(s string) string {
	var b bytes.Buffer
	for _, r := range s {
		if r != '"' && r != '\'' && r != '\n' {
			b.WriteRune(r)
		}
	}

	return b.String()
}

func grabStep2(ch1 <-chan string, w int, wg *sync.WaitGroup) {
	defer wg.Done()
	for val := range ch1 {
		fmt.Println("w ", w, "grabStep2: ", val)
		err := getProduct(val, w)
		if err != nil {
			fmt.Println("w ", w, "grabStep2: ", val, "err: ", err)
			saveErrorProductToFile(val, w)
		} else {
			fmt.Println("w ", w, "grabStep2: ", val)
		}
		runtime.Gosched()
	}
	fmt.Println("finish: ", w)
}

func getProduct(url string, w int) error {

	file := fmt.Sprintf(fileCsv, w)

	// пишем заголовок только в новый файл
	if _, err := os.Stat(file); os.IsNotExist(err) {
		productSlice := [][]string{{
			"sku",
			"name",
			"desc",
			"modeAppl",
			"composition",
			"indications",
			"length",
			"width",
			"height",
			"shippingWeight",
			"manufacturerCountry",
			"brand",
		}}
		saveProductCsv(productSlice, file)
	}

	url = strings.TrimSpace(url)
	//fmt.Println(detailURL)

	url = domen + url
	p, err := requestToDetail(url)

	if err != nil {
		return err
	}

	pDataForCsv := [][]string{{
		p.sku,
		p.name,
		p.desc,
		p.modeAppl,
		p.composition,
		p.indications,
		p.length,
		p.width,
		p.height,
		p.shippingWeight,
		p.manufacturerCountry,
		p.brand,
	}}
	saveProductCsv(pDataForCsv, file)
	return nil
}

// CurKey текущий ключ
var CurKey string
var propVal string

// MapValResult  хеш таблица результатов
var MapValResult = map[string]string{}

func main() {

	var jsonStr = `{"topList":false,"canExpand":false,"showLong":true,"withoutTitle":false,"limit":300,"externalDescription":null,"columns":2,"thresholdLength":166,"viewLine":"dotted","disclaimer":"Информация о технических характеристиках, комплекте поставки, стране изготовления, внешнем виде и цвете товара носит справочный характер и основывается на последних доступных к моменту публикации сведениях","title":"Характеристики","anchor":"section-characteristics","characteristics":[{"title":"","short":[{"key":"Type","name":"Тип","values":[{"text":"Кондиционер, ополаскиватель"}]},{"key":"TasteNF","name":"Аромат","values":[{"text":"Цветочный"}]},{"key":"Appointment","name":"Назначение.","values":[{"text":"Для детского белья"},{"text":"Для цветного белья"},{"text":"Для белого белья"},{"text":"Для одежды"}]},{"key":"Features","name":"Особенности применения","values":[{"text":"Концентрат"},{"text":"Гипоаллергенность"},{"text":"Антистатический эффект"}]},{"key":"EditionForm","name":"Форма выпуска","values":[{"text":"Жидкость"}]},{"key":"Country","name":"Страна-изготовитель","values":[{"text":"Германия"}]},{"key":"Article","name":"Артикул","values":[{"text":"830461;830461;830461;830461"}]},{"key":"TDimensions","name":"Размер упаковки (Длина х Ширина х Высота), см","values":[{"text":"12 x 8 x 28"}]},{"key":"LoundryMode","name":"Вид стирки","values":[{"text":"Ручная стирка"},{"text":"Автоматическая"}]},{"key":"BruttoWeight","name":"Вес в упаковке, г","values":[{"text":"1560"}]}]}],"cellTrackingInfo":{"uiMap":{"characteristicsTest":{"type":"ui","title":"Характеристики","elementType":"characteristicsTest","countItems":10,"sku":135027610}}}}`

	data := []byte(jsonStr)

	// Creating the maps for JSON
	//m := map[string]interface{}{}
	m := map[string]interface{}{}

	// Parsing/Unmarshalling JSON encoding/json
	err := json.Unmarshal(data, &m)

	if err != nil {
		panic(err)
	}

	fmt.Println(m)
	fmt.Println("*****************")

	m1 := m["characteristics"].([]interface{})
	fmt.Println(m1[0])
	fmt.Println("=============================")
	//fmt.Println(m1[0].(interface{}))

	for _, m2 := range m1 {
		fmt.Println(m2.(map[string]interface{}))
		for _, m3 := range m2.(map[string]interface{}) {
			switch concreteVal := m3.(type) {
			case []interface{}:
				//fmt.Println("KEY ", key)
				fmt.Println(m3.([]interface{}))

				for _, m4 := range m3.([]interface{}) {
					fmt.Println("==================")
					fmt.Println("m4: ", m4)

					switch concreteVal := m4.(type) {
					case map[string]interface{}:
						//fmt.Println("KEY ", key)
						//fmt.Println(m4.([]interface{}))

						for key5, m5 := range m4.(map[string]interface{}) {
							fmt.Println("m5: ", key5, m5)

							if key5 == "key" {
								CurKey = fmt.Sprintf("%v", m5)
							}

							switch concreteVal := m5.(type) {
							/*
								case map[string]interface{}:
									//fmt.Println("KEY ", key)
									//fmt.Println(m4.([]interface{}))

									for _, m6 := range m5.(map[string]interface{}) {
										fmt.Println("m6: ", m6)

									}*/
							case []interface{}:
								//fmt.Println("Index:", i)
								fmt.Println(m5.([]interface{}))

								for key6, m6 := range m5.([]interface{}) {
									fmt.Println("m6: ", m6)

									switch concreteVal := m6.(type) {
									case map[string]interface{}:
										propVal = ""
										for _, m7 := range m6.(map[string]interface{}) {
											propVal += fmt.Sprintf("%v", m7) + " "
											fmt.Println("m7: ", m7)

										}
									default:
										fmt.Println("key6: ", key6, concreteVal)

										//fmt.Println(concreteVal)
									}

								}

							default:
								fmt.Println("key5: ", key5, concreteVal)

								//fmt.Println(concreteVal)
							}

						}

					default:
						fmt.Println(concreteVal)
					}

					// запись разобранного свойства
					MapValResult[CurKey] = propVal

				}

			default:
				fmt.Println(concreteVal)
			}

			//fmt.Printf("%T\n", m3)
			//fmt.Println("m3:", m3)
			//fmt.Println("m3!!!:", m3.(map[string]string))
			// for _, m4 := range m3.(map[string]interface{}) {
			// 	fmt.Println("m4: ", m4)
			// }

			//os.Exit(1)
		}
	}

	//MapValResult := make(map[string]string)
	//CurKey := ""
	//parseMap(m)

	//fmt.Println(CurKey)
	fmt.Println(MapValResult)

	///var dataChar interface{}

	//json.Unmarshal(data, dataChar)

	//fmt.Printf("struct:\n\t%#v\n\n", dataChar)

	os.Exit(1)

	flag.IntVar(&step, "step", step, "шаг 1 - формирование url товаров, шаг 2 - формирование csv данных")
	flag.IntVar(&workers, "w", workers, "количество потоков")
	flag.IntVar(&reportPeriod, "r", reportPeriod, "частота отчетов (сек)")
	flag.StringVar(&fileStartURL, "fstart", fileStartURL, "файл c сылками категорий")
	flag.StringVar(&fileCsv, "fcsv", fileCsv, "файл данных по товарам")
	flag.StringVar(&fileURL, "furl", fileURL, "файл URL по товарам")

	flag.Parse()

	ticker := time.NewTicker(time.Duration(reportPeriod) * time.Second)
	defer ticker.Stop()

	if step == 1 {
		fmt.Println("Step", step)
		getProductList()
	} else {
		fmt.Println("Step", step)

		ch1 := make(chan string, 1)
		wg := &sync.WaitGroup{}

		for i := 0; i < workers; i++ {
			wg.Add(1)
			go grabStep2(ch1, i, wg)
		}

		lines, err := readLines(fileURL)
		check(err)

		for _, value := range lines {
			value = strings.TrimSpace(value)
			ch1 <- value
		}
		close(ch1)
		//time.Sleep(time.Millisecond)
		wg.Wait()
		// сканирование без воркеров
		//getProductData()
	}
}

func parseMap(aMap map[string]interface{}) {
	for key, val := range aMap {
		switch concreteVal := val.(type) {
		case map[string]interface{}:
			fmt.Println("KEY ", key)
			parseMap(val.(map[string]interface{}))
		case []interface{}:
			fmt.Println("KEY ", key)
			parseArray(val.([]interface{}))
		default:
			if key == "key" {
				CurKey = fmt.Sprintf("%v", concreteVal)
			}

			if key == "text" {
				MapValResult[CurKey] = fmt.Sprintf("%v", concreteVal)
			}

			fmt.Println(key, "-:-", concreteVal)
		}
	}
}

func parseArray(anArray []interface{}) {
	for i, val := range anArray {
		switch concreteVal := val.(type) {
		case map[string]interface{}:
			fmt.Println("Index:", i)
			parseMap(val.(map[string]interface{}))
		case []interface{}:
			fmt.Println("Index:", i)
			parseArray(val.([]interface{}))
		default:
			fmt.Println("Index", i, ":", concreteVal)

		}
	}
}
