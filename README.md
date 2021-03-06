# APMC_TelenganaScrapping
#### Scrapping Agriculture data from APMC Telangana website using Selenium library, then clean that raw data using Pandas library in Python
#### [APMC Telengana Website](http://tsmarketing.in/HomePageGe.aspx)
1. The Code is given in jupyter Notebook.ipynb and .py format.
2. Directory path of **```Gramoday_Datasets_market.xlsx```** file need to be changed while running on other machine.
3. **```Gramoday_Datasets_market.xlsx```** file is attached.



```Python version 3.7.9 (default, Aug 31 2020, 17:10:11) [MSC v.1916 64 bit (AMD64)] Version info. sys.version_info(major=3, minor=7, micro=9, releaselevel='final', serial=0)```
1. ```open Anaconda prompt, change file directory path i.e., cd path_to_folder```
2. **```conda create -n apmcTelenganaScrapping python=3.7```**
3. **```conda activate apmcTelenganaScrapping```**
4. **```conda install spyder```**
5. ```pip install -r path_of_requirments.txt```
6. **```selenium==3.141.0```**
7. **```beautifulsoup4==4.9.3```**
8. **```pandas==1.2.0```**
9. After above installation type **```spyder```** in Anaconda prompt. Now Spyder IDE will open in sometime.
10. open **```telengana_scrap_final.py```** in spyder 

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
### Screenshots of works done
1. #### **Website Home Page**
![Website Home Page](https://github.com/sanjeebKumarGouda/APMC_TelenganaScrapping/blob/main/resources/1.png)
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
2. #### **Website Market Page**
![Website Market Page](https://github.com/sanjeebKumarGouda/APMC_TelenganaScrapping/blob/main/resources/2.png)
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
3. #### **Website Data Page (This Tabular data needs to be extracted)**
![Website Data Page (This Tabular data needs to be extracted)](https://github.com/sanjeebKumarGouda/APMC_TelenganaScrapping/blob/main/resources/3.png)
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
4. #### **Extracted Raw Data**
![Extracted Raw Data](https://github.com/sanjeebKumarGouda/APMC_TelenganaScrapping/blob/main/resources/4.png)
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
5. #### **Extracted Cleaned Data**
![Extracted Cleaned Data](https://github.com/sanjeebKumarGouda/APMC_TelenganaScrapping/blob/main/resources/5.png)
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
