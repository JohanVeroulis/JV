import pandas as pd 
import numpy as np
import plotly.express as px
import plotly.graph_objects as go


#file 
projectpath = "/Users/ioannisveroulis/MyProjects/"
excel_file = projectpath + 'data/apple_products.xlsx'

#read xlsx file, read data 
df = pd.read_excel(excel_file)

#read csv file 
#data = pd.read_csv("apple_products.csv")

#display the first ten rows of file 
#print(df.head()) 


#if this file has many numbres of null(0) this funnction disappear them 
print(df.isnull().sum())

#describe the product of file 
print(df.describe())


highest_rating = df.sort_values(by=["Star Rating"],ascending=False)
highest_rating = highest_rating.head(10)
print(highest_rating['Product Name'])


#create a figure 
iphones = highest_rating["Product Name"].value_counts()
label = iphones.index
counts = highest_rating["Number Of Ratings"]
figure = px.line_3d(highest_rating, x=label, y=counts, title="Number of Ratings of Highest Rated iPhones")

#save figure as .png
#figure = figure.write_image("reports/figure1.png")

#save figure as .pdf
#figure = figure.write_image("reports/figure3.pdf")

#save figure as .jpeg
#figure = figure.write_image("reports/figure4.jpeg")

#save figure as .html
#figure = figure.write_html("test.html")
figure.show()





figure1 = px.scatter(data_frame=df, x="Number Of Ratings", 
                    #y="Sale Price", size="Discount Percentage", 
                    #trendline="ols", 
                    #title="Relationship between Sale Price and Number of Ratings of iPhones")

#figure1.show()

