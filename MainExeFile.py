from FTPConnector import FTPConnector
from Utility import Utility
from DbHelper import MySqlDbHelper
from Models import *



downloadFileList = ['cc_score','channel_summary','market_summary']
localFTPFolderPath = r'C:\Users\jlx\Desktop\VTraining\Python\Build up Customer Signature\workspace\CIKCSPython\CIKTel\ScottVersion\testFolder'
fileDateStr = '20160207'

#Downloading files from FTP
def DownloadFTPFiles(ftpFolderPath, fileList):    
    myFtpConn = FTPConnector('ftp.jobready123.com','dq121@testingmoo.com', 'K#Xwx3Sp.gmL')
    for fn in fileList:
        tfpFn = Utility.FormatFtpCsvFileNameByDate(fn,fileDateStr)
        myFtpConn.DownloadFile(tfpFn,ftpFolderPath,'/DataAnalysis/')

#Imort data Models
def ImportDataToDB(ftpFolderPath, fileList):    
    myDbHelper = MySqlDbHelper("localhost","root","","cikcs")
    ImportStagingCcScore(myDbHelper,ftpFolderPath,fileList[0])
    ImportChannelSummary(myDbHelper,ftpFolderPath,fileList[1])
    ImportMarketSummary(myDbHelper,ftpFolderPath,fileList[2])
    ImportCsCustomer(myDbHelper)
    ImportCsInitTran(myDbHelper)
    ImportCsSubs(myDbHelper)
    ImportCsOrder(myDbHelper)

def ImportCsOrder(dbHelper):
    dbHelper.ExecuteNonQuery(['delete from cikcs.cs_order'])
    insertSqlList = []
    insertSqlList.append(r"""INSERT INTO `cikcs`.`cs_order`
                                (`customerid`,
                                `zipcode`,
                                `state`,
                                `paymenttype`,
                                `totalOrders`,
                                `avgOrderSize`,
                                `totalPrice`)
                               SELECT customerid, zipcode, state, paymenttype, count(*) as totalOrders, 
                                avg(totalprice) as avgOrderSize, sum(totalprice) as totalprice
                                 from sqlbook.orders 
                                 where customerid in 
                                 (SELECT distinct customer_id from cikcs.file_customer_credit_score)
                                 group by customerid""")
    dbHelper.ExecuteNonQuery(insertSqlList)

def ImportCsSubs(dbHelper):
    dbHelper.ExecuteNonQuery(['delete from cikcs.cs_subs'])
    insertSqlList = []
    insertSqlList.append(r"""INSERT INTO `cikcs`.`cs_subs`
	                            (`customerid`,
	                            `market`,
	                            `channel`,
	                            `tenure`,
	                            `m_avg_tenure`,
	                            `m_avg_ordersize`,
	                            `c_avg_tenure`,
	                            `c_avg_ordersize`)
                            SELECT customer_id, sub.market, sub.channel, tenure, ms.avg_tenure,ms.avg_order_size, cs.avg_tenure,cs.avg_order_size
	                            from sqlbook.subs as sub
	                            Left Join cikcs.file_marketing_summary as ms ON ms.market = sub.market
	                            Left Join cikcs.file_channel_summary as cs ON cs.channel = sub.channel
	                            where customer_id in (SELECT distinct customer_id from cikcs.file_customer_credit_score)""")
    dbHelper.ExecuteNonQuery(insertSqlList)

def ImportCsInitTran(dbHelper):
    dbHelper.ExecuteNonQuery(['delete from cikcs.cs_init_tran'])
    insertSqlList = []
    insertSqlList.append(r"""INSERT INTO `cikcs`.`cs_init_tran`
                                (`customerid`,
                                `orderid`,
                                `initdate`,
                                `product_group_name`)
                                SELECT ol.customerid, ol.orderid, ol.initdate, product.productgroupname from 
	                            (select customerid, orders.orderid, min(shipdate) as initdate, productid from sqlbook.orders, sqlbook.orderline
	                             where orders.orderid = orderline.orderid and customerid in (SELECT distinct customer_id from cikcs.file_customer_credit_score)
	                             group by orders.customerid 
	                             order by min(shipdate) 
	                             ) ol 
	                             join sqlbook.product 
	                             where ol.productid = product.productid""")
    dbHelper.ExecuteNonQuery(insertSqlList)

def ImportCsCustomer(dbHelper):
    dbHelper.ExecuteNonQuery(['delete from cikcs.cs_customer'])
    insertSqlList = []
    insertSqlList.append(r"""INSERT INTO `cikcs`.`cs_customer`
                            (`customerid`,`householdid`,`gender`,`name`)
                            SELECT `customer`.`customerid`,
                                `customer`.`householdid`,
                                `customer`.`gender`,
                                `customer`.`firstname`
                            FROM `sqlbook`.`customer`
                            Where `customer`.`customerid` in 
                            (SELECT distinct customer_id from cikcs.file_customer_credit_score)""")
    dbHelper.ExecuteNonQuery(insertSqlList)

def ImportStagingCcScore(dbHelper, ftpFolderPath, fileName):
    readLines = Utility.ReadLinesFromFile(ftpFolderPath + '\\' + Utility.FormatFtpCsvFileNameByDate(fileName,fileDateStr))
    insertSqlList = []
    for ln in readLines:
        lnStr = ln.strip()
        if lnStr != '':
            valList = lnStr.split(",")
            customerCreditScoreModel = CustomerCreditScore('file_customer_credit_score',valList[1],valList[2])
            insertSqlList.append(customerCreditScoreModel.GetInsertSqlString())    
    if len(insertSqlList) > 0:
        dbHelper.ExecuteNonQuery(['delete from file_customer_credit_score'])
        dbHelper.ExecuteNonQuery(insertSqlList)

def ImportChannelSummary(dbHelper, ftpFolderPath, fileName):
    readLines = Utility.ReadLinesFromFile(ftpFolderPath + '\\' + Utility.FormatFtpCsvFileNameByDate(fileName,fileDateStr))
    insertSqlList = []
    for ln in readLines:
        lnStr = ln.strip()
        if lnStr != '':
            valList = lnStr.split(",")
            channelSummaryModel = ChannelSummary('file_channel_summary',valList[1],valList[2],valList[3])
            insertSqlList.append(channelSummaryModel.GetInsertSqlString())    
    if len(insertSqlList) > 0:
        dbHelper.ExecuteNonQuery(['delete from file_channel_summary'])
        dbHelper.ExecuteNonQuery(insertSqlList)

def ImportMarketSummary(dbHelper, ftpFolderPath, fileName):
    readLines = Utility.ReadLinesFromFile(ftpFolderPath + '\\' + Utility.FormatFtpCsvFileNameByDate(fileName,fileDateStr))
    insertSqlList = []
    for ln in readLines:
        lnStr = ln.strip()
        if lnStr != '':
            valList = lnStr.split(",")
            marketSummaryModel = MarketingSummary('file_marketing_summary',valList[1],valList[2],valList[3])
            insertSqlList.append(marketSummaryModel.GetInsertSqlString())    
    if len(insertSqlList) > 0:
        dbHelper.ExecuteNonQuery(['delete from file_marketing_summary'])
        dbHelper.ExecuteNonQuery(insertSqlList)

def InsertCustomerSignature():
    myDbHelper = MySqlDbHelper("localhost","root","","cikcs")

    sqlStr = r"SELECT * FROM cikcs.cs_init_tran;"
    csInitTranResults = myDbHelper.ExecuteQuery(sqlStr)

    sqlStr = r"SELECT * FROM cikcs.cs_customer;"
    csCustomerResults = myDbHelper.ExecuteQuery(sqlStr)
    csCustomerResultsDict = {}
    for row in csCustomerResults:
        csCustomerResultsDict[row[0]] = row #key:CustomerID

    sqlStr = r"SELECT * FROM cikcs.cs_Order;"
    csOrderResults = myDbHelper.ExecuteQuery(sqlStr)
    csOrderResultsDict = {}
    for row in csOrderResults:
        csOrderResultsDict[row[0]] = row #key:CustomerID

    sqlStr = r"SELECT * FROM cikcs.cs_subs;"
    csSubsResults = myDbHelper.ExecuteQuery(sqlStr)
    csSubsResultsDict = {}
    for row in csSubsResults:
        csSubsResultsDict[row[0]] = row #key:CustomerID

    sqlStr = r"SELECT zipcode, hhuoccupied,hhunmarried,hhmedincome,fammedincome,popedu FROM cikcs.cs_zipcensus WHERE zipcode in (SELECT zipcode from cikcs.cs_order);"
    csZipcensusResults = myDbHelper.ExecuteQuery(sqlStr)
    csZipcensusResultsDict = {}
    for row in csZipcensusResults:
        csZipcensusResultsDict[row[0]] = row #key:zipcode

    if len(csInitTranResults) > 0:
        insertSqlList = []
        for ord in csInitTranResults:
            customerSignatureModel = CreateCustomerSignatureInsertSql(ord, csCustomerResultsDict,csOrderResultsDict, csSubsResultsDict, csZipcensusResultsDict)
            insertSqlList.append(customerSignatureModel.GetInsertSqlString())
        
        if len(insertSqlList) > 0:
            myDbHelper.ExecuteNonQuery(['delete from customersignature'])
            myDbHelper.ExecuteNonQuery(insertSqlList)

def CreateCustomerSignatureInsertSql(csInitTranRecord, csCustomerResultsDict, csOrderResultsDict, csSubsResultsDict, csZipcensusResultsDict):
    orderId = csInitTranRecord[1]
    custId = csInitTranRecord[0]
    customerSignatureModel = CustomerSignature('customersignature',orderId,custId)
    customerSignatureModel.SetInitDate(csInitTranRecord[2])
    customerSignatureModel.SetProductGroupName(csInitTranRecord[3])

    # from cs_customer
    SetCustomerSignatureFromCustomer(custId, customerSignatureModel, csCustomerResultsDict)
    # from cs_Order
    zipCode = SetCustomerSignatureFromOrder(custId, customerSignatureModel, csOrderResultsDict)
    # From cs_subs
    SetCustomerSignatureFromSubs(custId, customerSignatureModel, csSubsResultsDict)
    # From cs_zipcensus
    SetCustomerSignatureFromZipcensus(zipCode, customerSignatureModel, csZipcensusResultsDict)

    return customerSignatureModel

def SetCustomerSignatureFromCustomer(custId, customerSignatureModel, csCustomerResultsDict):
    customerSignatureModel.SetHouseholdId(csCustomerResultsDict[custId][1])
    customerSignatureModel.SetGender(csCustomerResultsDict[custId][2])
    customerSignatureModel.SetName(csCustomerResultsDict[custId][3])

def SetCustomerSignatureFromOrder(custId, customerSignatureModel, csOrderResultsDict):
    customerSignatureModel.SetZipcode(csOrderResultsDict[custId][1])
    customerSignatureModel.SetState(csOrderResultsDict[custId][2])
    customerSignatureModel.SetPaymentType(csOrderResultsDict[custId][3])
    customerSignatureModel.SetTotalOrders(csOrderResultsDict[custId][4])
    customerSignatureModel.SetAvgOrderSize(csOrderResultsDict[custId][5])
    customerSignatureModel.SetTotalPrice(csOrderResultsDict[custId][6])
    return csOrderResultsDict[custId][1]

def SetCustomerSignatureFromSubs(custId, customerSignatureModel, csSubsResultsDict):
    customerSignatureModel.SetMarket(csSubsResultsDict[custId][1])
    customerSignatureModel.SetChannel(csSubsResultsDict[custId][2])
    customerSignatureModel.SetTenure(csSubsResultsDict[custId][3])
    customerSignatureModel.SetMarketAvgTenure(csSubsResultsDict[custId][4])
    customerSignatureModel.SetMarketAvgOrdersize(csSubsResultsDict[custId][5])
    customerSignatureModel.SetChannelAvgTenure(csSubsResultsDict[custId][6])
    customerSignatureModel.SetChannelAvgOrdersize(csSubsResultsDict[custId][7])

def SetCustomerSignatureFromZipcensus(zipcode, customerSignatureModel, csZipcensusResultsDict):
    customerSignatureModel.SetHhuOccupied(csZipcensusResultsDict[zipcode][1])
    customerSignatureModel.SetHhunMarried(csZipcensusResultsDict[zipcode][2])
    customerSignatureModel.SetHhMedIncome(csZipcensusResultsDict[zipcode][3])
    customerSignatureModel.SetFamMedIncome(csZipcensusResultsDict[zipcode][4])
    customerSignatureModel.SetPopEdu(csZipcensusResultsDict[zipcode][5])

#Start from here!!!

print ("Hello World")
#DownloadFTPFiles(localFTPFolderPath, downloadFileList)
#ImportDataToDB(localFTPFolderPath,downloadFileList)
##Looking up the data for populating customersignature
#InsertCustomerSignature()
