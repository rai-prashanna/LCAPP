import re

def printMapper(mapper):
    for key in mapper:
        print(f"{key}: {mapper.get(key)}")

def LoadItemIdItemName():
    rawoutputfile = open("sales_data/item_categories.csv", "r")
    mapper={}
    next(rawoutputfile)
    for line in rawoutputfile:
        itemid, itemname = line.split(',')
        mapper[itemid] = itemname.rstrip("\n")
    return mapper

def LoadCategoryMapper():
    rawoutputfile = open("sales_data/categories.csv", "r")
    mapper={}
    next(rawoutputfile)
    for line in rawoutputfile:
        Category, Subcategory = line.split(',')
        trimmedKey=Subcategory.rstrip("\n")
        mapper[trimmedKey] = Category
    return mapper

rawoutputfile = open("assingment1.out/part-00000", "r")
SalesRecordMapper = {}
ItemCategoryMapper=LoadItemIdItemName()

for line in rawoutputfile:
    result = re.sub('[()]', '', line)
    itemid, sales = result.split(',')
    itemid=itemid.strip('\'')
    sales=sales.strip(' ').rstrip("\n")
    itemName=ItemCategoryMapper.get(itemid)
    if itemName in SalesRecordMapper:
        previous_sales=SalesRecordMapper[itemName]
        SalesRecordMapper[itemName]=previous_sales+int(sales)
    else:
        SalesRecordMapper[itemName] = int(sales)

printMapper(SalesRecordMapper)
CategoryMapper = LoadCategoryMapper()


def SubtotalingleafToIntermediateRoot(CategoryMapper):
    mapper = {}
    SalesReportMapper={}
    for itemName in SalesRecordMapper:
        subtotal=SalesRecordMapper[itemName]
        category=CategoryMapper[itemName]
        if category in SalesReportMapper:
            previous_value=SalesReportMapper[category]
            SalesReportMapper[category]=previous_value+subtotal
        else:
            SalesReportMapper[category] = subtotal
    return SalesReportMapper


SalesReportMapper=SubtotalingleafToIntermediateRoot(CategoryMapper)

def SubtotalingIntermediateRootToroot1(SalesReportMapper,CategoryMapper):
    mapper = {}
    for itemName in SalesReportMapper:
        subtotal=SalesReportMapper[itemName]
        category=CategoryMapper[itemName]
        if category in SalesReportMapper:
            previous_value=SalesReportMapper[category]
            SalesReportMapper[category]=previous_value+subtotal
        else:
            SalesReportMapper[category] = subtotal
        return SalesReportMapper

temp=SubtotalingIntermediateRootToroot1(SalesReportMapper,CategoryMapper)
print(temp)
