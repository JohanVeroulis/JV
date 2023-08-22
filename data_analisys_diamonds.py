import pandas as pd 
import matplotlib.pyplot as plt 
import seaborn as sns 


#path file 
projectPath="/Users/ioannisveroulis/MyProjects/"
csv_file = projectPath + 'data/diamonds.csv'

#read file
df = pd.read_csv(csv_file)


#print(df.head())

dfpremium = (df[(df['cut']=='Premium') & (df['color']=='E')].sort_values('carat'))
#print(dfpremium)

#print(dfpremium['table'].unique())


sns.pairplot(dfpremium, hue='clarity')
plt.show()



#print(df[df['carat']=="0.20"].sort_values('price',ascending=False).head(1))

#price_1 = df.groupby(['price','carat']).mean()

#print(price_1)

#print(price_1['price'].unique())

#print(df['color'].unique())

# sns.barplot(x='color',y='carat',data=df)
# plt.show()

#sns.distplot(df['price'],kde=False)
#plt.show()


# sns.countplot(x='cut',data=df)
# plt.show()

# sns.distplot(df['carat'])
# plt.show()
# sns.countplot(x='color',data=df,hue='clarity')
# plt.show()


# plt.plot(x='cut')
# plt.show()

