import unittest
import os
os.environ['JAVA_HOME']='/usr/lib/jvm/java-11-openjdk-amd64'
import helper

class ITestsaleReport(unittest.TestCase):

    def test_total_sale_report_of_Clothes(self):
        """Positive Test-Case that checks total sale made by Clothes."""
        print("*********************************************************************")
        print("testing****")
        ItemCategoryMapper = helper.LoadItemIdItemName("test_data/item_categories.csv")
        SalesbyName = helper.MapItemIDtoName(ItemCategoryMapper, "test.assingment1.out/part-00000")
        CategoryMapper = helper.LoadCategoryMapper("test_data/categories.csv")
        salesofEachSubCategory = helper.SalesofEachSubCategory(SalesbyName, CategoryMapper)
        salesOfEachCategory = helper.salesOfEachCategory(salesofEachSubCategory, CategoryMapper)
        print(SalesbyName)
        print("***************************************************")
        print("Electrical:"+str(salesofEachSubCategory[('Electrical')]))
        print("***************************************************")
        print("Clothes:"+str(salesOfEachCategory[('Clothes')]))
        print("Books:"+str(salesOfEachCategory[('Books')]))
        print("White Goods:"+str(salesOfEachCategory[('White Goods')]))
        print("Clothes:"+str(salesOfEachCategory[('Clothes')]))
        print("Mens Clothes:"+str(salesofEachSubCategory[('Mens Clothes')]))
        print("Womens Clothes:"+str(salesofEachSubCategory[('Womens Clothes')]))
        print("All :"+str(salesOfEachCategory[('All')]))

        self.assertEqual(12000, salesOfEachCategory[('Clothes')])
        self.assertEqual(33600, salesOfEachCategory[('All')])
        self.assertEqual(9600, salesOfEachCategory[('Books')])
        self.assertEqual(6000, salesofEachSubCategory[('Electrical')])
        self.assertEqual(6000, salesOfEachCategory[('White Goods')])
