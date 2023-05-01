# WCPFC_Crawler #
Crawler WCPRC IUU List.</br>
A program for Crawler, Please have know some instruction with this README.md

## Part I: Auto Download WCPFC List with Excel file ##
Download WCFP List Excel file and Clean it then Storage data to database after comparison.

### Basic Info ###
+ Org = WCPFC
+ Executing every week

### How it Run ###
1. Find `exe_getExcel.bat`
2. Setting Task Scheduler interval once a week on `exe_getExcel.bat`

## Part II： Get WCPFC List for **[ getWCPFCList.py ]** ##
Crawler WCPFC RFMO List and obtain *Vessel Name* and *URL* <br>
Before executing program, you need to figure out the page range.

### Basic Info ###
+ Org = WCPFC
+ FIX_URL = `https://www.wcpfc.int/record-fishing-vessel-database?page=`

### How to Use ###
1. Open file.
2. Find as below at `def run`
> ```python
> datas = crawler.getWebNum(st=0, ed=None)
> ```
> `st` will be the start page number
> or it also can give end page number by parameter `ed`
> > e.g. Crawling page 1 to 5 `crawler.getWebNum(st=0, ed=4)`
3. Save File and Open cmd and enter to Python Environment
4. **At Python Env in cmd**, key in
> ```python
> import getWCPFCList
> getWCPFCList.run()
> ```
5. Waiting it until Finished, and the result will be save at `result.json`

### Result Format ###
result is a JSON format that {"datas": type[List]}
> List[ORG, WebURL, IMO(fake), VWLName1]
