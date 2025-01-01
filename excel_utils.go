package main

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/xuri/excelize/v2"
	"log"
	"net/url"
	"os"
	"strconv"
	"time"
)

func ToExcelBySlice(columns []string, datas [][]interface{}, sheetName, addSheet string, f *excelize.File) (*excelize.File, error) {
	if addSheet == "" && f == nil {
		f = excelize.NewFile()
	}
	if addSheet != "" {
		f.NewSheet(addSheet)
		convert, err := excelConvert(f, columns, datas, addSheet)
		if err != nil {
			return nil, err
		}
		return convert, err
	}
	f.SetSheetName("Sheet1", sheetName)
	convert, err := excelConvert(f, columns, datas, sheetName)
	if err != nil {
		return nil, err
	}
	return convert, err
}

func excelConvert(f *excelize.File, columns []string, datas [][]interface{}, sheetName string) (*excelize.File, error) {
	header := make([]string, 0)
	for _, v := range columns {
		header = append(header, v)
	}
	titleStyle := &excelize.Style{
		Font: &excelize.Font{
			Family: "arial",
			Size:   16,
			Color:  "#2561cb",
		},
		Alignment: &excelize.Alignment{
			Vertical:   "center",
			Horizontal: "center",
		},
	}
	rowStyle := &excelize.Style{
		Font: &excelize.Font{
			Family: "arial",
			Size:   13,
			Color:  "#666666",
		},
		Alignment: &excelize.Alignment{
			Vertical:   "center",
			Horizontal: "center",
		},
	}
	titleStyleID, _ := f.NewStyle(titleStyle)
	rowStyleID, _ := f.NewStyle(rowStyle)
	_ = f.SetSheetRow(sheetName, "A1", &header)
	_ = f.SetRowHeight(sheetName, 1, 30)
	length := len(columns)
	headStyle := letter(length)
	var lastRow string
	var widthRow string
	for k, v := range headStyle {
		if k == length-1 {
			lastRow = fmt.Sprintf("%s1", v)
			widthRow = v
		}
	}
	if err := f.SetColWidth(sheetName, "A", widthRow, 30); err != nil {
		return nil, err
	}
	rowNum := 1
	if len(datas) < 1 {
		if err := f.SetCellStyle(sheetName, fmt.Sprintf("A1"), fmt.Sprintf("%s", lastRow), titleStyleID); err != nil {
			return nil, err
		}
	}
	for _, data := range datas {
		row := make([]interface{}, 0)
		for _, val := range data {
			row = append(row, val)
		}
		rowNum++
		if err := f.SetSheetRow(sheetName, fmt.Sprintf("A%d", rowNum), &row); err != nil {
			return nil, err
		}
		if err := f.SetCellStyle(sheetName, fmt.Sprintf("A%d", rowNum), fmt.Sprintf("%s", lastRow), rowStyleID); err != nil {
			return nil, err
		}
		if err := f.SetCellStyle(sheetName, fmt.Sprintf("A1"), fmt.Sprintf("%s", lastRow), titleStyleID); err != nil {
			return nil, err
		}
	}
	return f, nil
}

func letter(length int) []string { // 遍历a-z
	var str []string
	for i := 0; i < length; i++ {
		str = append(str, string(rune('A'+i)))
	}
	return str
}

func DownloadExcel(c *gin.Context, f *excelize.File, fileName string) error {
	disposition := fmt.Sprintf("attachment; filename=%s.xlsx", url.QueryEscape(fileName))
	c.Writer.Header().Set("Content-Type", "application/octet-stream")
	c.Writer.Header().Set("Content-Disposition", disposition)
	b, err := f.WriteToBuffer()
	if err != nil {
		return err
	}
	c.Writer.Header().Set("Content-Length", strconv.Itoa(b.Len()))
	_, err = c.Writer.Write(b.Bytes())
	if err != nil {
		return err
	}
	return nil
}

func ExportExcel(path, fileName string, f *excelize.File) error {
	var exists bool
	fileInfo, err := os.Stat(path + fileName + ".xlsx")
	if fileInfo != nil && err == nil {
		// 文件存在
		exists = true
	}
	if exists {
		timeStamp := strconv.Itoa(int(time.Now().Unix()))
		fileName = fileName + "-" + timeStamp
	}
	defer func() {
		f.Close()
	}()
	err = f.SaveAs(path + fileName + ".xlsx")
	if err != nil {
		return err
	}
	return nil
}

// GetXlsxIndexLetter 根据行的index获取行字母
func GetXlsxIndexLetter(rowIndex int) string {
	var Letters = []string{"A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z"}
	result := Letters[rowIndex%26]
	rowIndex = rowIndex / 26
	for rowIndex > 0 {
		rowIndex = rowIndex - 1
		result = Letters[rowIndex%26] + result
		rowIndex = rowIndex / 26
	}

	return result
}

type SaveXlsxData struct {
	SheetName   string          // sheet名称：默认Sheet1
	Title       []XlsxTitles    // 表头配置
	TitleHeight float64         // 首行标题高度
	Rows        [][]interface{} // 每一行的数据
	StartIndex  int             // 从第几行开始写数据
}

// excel表格标题
type XlsxTitles struct {
	Title string `json:"title"` // 标题
}

var defaultTitleHeight float64 = 24

// SaveXlsxFootPlus 落盘保存表格(支持多sheet,sheetName必填)
func SaveXlsxFootPlus(dataList []SaveXlsxData, filePath string) (f *excelize.File, err error) {
	if filePath == "" {
		err = fmt.Errorf("没有配置文件生成路径")
		return
	}
	// 会自带Sheet1，后面程序进行了删除
	f = excelize.NewFile()
	for _, data := range dataList {
		xlsxTitleLen := len(data.Title)
		xlsxTitle := make([]interface{}, 0, xlsxTitleLen)
		xlsxColWidth := make(map[string]float64, xlsxTitleLen)
		for rowIndex, v := range data.Title {
			xlsxTitle = append(xlsxTitle, v.Title)
			xlsxColWidth[GetXlsxIndexLetter(rowIndex)] = float64(len([]rune(v.Title)) * 3)
		}

		xlsxRowsLen := len(data.Rows)

		// 生成xlsx
		sheetName := data.SheetName
		f.NewSheet(sheetName)
		// 创建工作簿
		f.SetRowHeight(sheetName, 1, defaultTitleHeight)
		if data.TitleHeight > defaultTitleHeight {
			f.SetRowHeight(sheetName, 1, data.TitleHeight)
		}

		// 写入表头
		addr, errExcelTitle := excelize.JoinCellName("A", 1)
		if errExcelTitle != nil {
			err = fmt.Errorf("文件生成错误：%+v", errExcelTitle.Error())
			return
		}
		if err = f.SetSheetRow(sheetName, addr, &xlsxTitle); err != nil {
			err = fmt.Errorf("文件生成错误：%+v", err.Error())
			return
		}

		// 写入rows数据
		if xlsxRowsLen > 0 {
			//第几行开始写入
			startIndex := 2
			if data.StartIndex > 0 {
				startIndex = data.StartIndex
			}
			for rowIdx, v := range data.Rows {
				rowAddr, rowErr := excelize.JoinCellName("A", rowIdx+startIndex)
				if rowErr != nil {
					err = fmt.Errorf("文件生成错误：%+v", rowErr.Error())
					return
				}
				if err = f.SetSheetRow(sheetName, rowAddr, &v); err != nil {
					err = fmt.Errorf("文件生成错误：%+v", err.Error())
					return
				}
			}
		}
		headerStyle, styleErr := f.NewStyle(&excelize.Style{
			Alignment: &excelize.Alignment{
				Vertical:   "center",
				Horizontal: "center",
			}},
		)
		if styleErr != nil {
			err = fmt.Errorf("文件生成错误：%+v", styleErr.Error())
			return
		}

		// 为标题行设置样式
		if err = f.SetCellStyle(sheetName, "A1", fmt.Sprintf("%s1", GetXlsxIndexLetter(xlsxTitleLen-1)), headerStyle); err != nil {
			err = fmt.Errorf("文件生成错误：%+v", err.Error())
			return
		}
	}

	f.DeleteSheet("Sheet1")
	// 保存
	if err = f.SaveAs(filePath); err != nil {
		err = fmt.Errorf("文件生成错误：%+v", err.Error())
		return
	}

	return
}

// FileStruct xlsx文件解析
type FileStruct struct {
	SheetName      string `json:"sheet_name"`
	RawCellValue   bool   `json:"raw_cell_value"`
	IsFirstSheet   bool   `json:"is_first_sheet"`
	TitleStartLine int64
}

func ExcelParseExt(fileName string, title map[string]string, args ...FileStruct) (resourceArr []map[string]string, titleMap map[string]bool, titleSort []string, err error) {
	f, err := excelize.OpenFile(fileName)
	if err != nil {
		return
	}
	defer func() {
		// Close the spreadsheet.
		if err = f.Close(); err != nil {
			log.Println(err)
		}
	}()
	tmpTitle := make(map[int]string, len(title))
	for _, sheet := range f.GetSheetList() {
		if len(args) > 0 && args[0].SheetName != sheet {
			continue
		}
		rows, rowErr := f.GetRows(sheet, excelize.Options{
			RawCellValue: true,
		})
		if rowErr != nil {
			err = rowErr
			return
		}
		titleMap = make(map[string]bool, len(rows))
		resourceArr = make([]map[string]string, 0, len(rows))
		var titleLen int
		for index, row := range rows {
			// 默认第一行为表头,组合表头需要定义行数
			if index == 0 {
				titleLen = len(row)
				if title == nil {
					tmpTitle = make(map[int]string, titleLen)
				}
				for rowIndex, colCell := range row {
					titleMap[colCell] = false
					if title == nil {
						tmpTitle[rowIndex] = colCell
						titleSort = append(titleSort, colCell)
						continue
					}
					if t, ok := title[colCell]; ok {
						tmpTitle[rowIndex] = t
					}
				}

				continue
			}

			tmpArr := make(map[string]string, titleLen)
			tmpRowLen := len(row)
			for i := 0; i < titleLen; i++ {
				var tmpRowData string
				if tmpRowLen >= i+1 {
					tmpRowData = row[i]
				}
				if _, ok := tmpTitle[i]; ok {
					tmpArr[tmpTitle[i]] = tmpRowData
				}
			}
			resourceArr = append(resourceArr, tmpArr)
		}

		// 只解析一个sheet
		break
	}

	return
}
