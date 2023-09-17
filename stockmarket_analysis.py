import pandas as pd 
import yfinance as yf 
import datetime 
from datetime import date, timedelta
import plotly.express as px
import plotly.graph_objects as go



today = date.today()

d1 = today.strftime("%Y-%m-%d")
end_date = d1
d2 = date.today() - timedelta(days=365)
d2 = d2.strftime("%Y-%m-%d")
start_date = d2

#download data from google
data = yf.download('GOOG',start=start_date,end=end_date,progress=False)

data["Date"] = data.index 

#what objects i want to put in my diagram 
data = data [["Date","Open","High","Low","Close","Volume"]]


data.reset_index(drop=True, inplace=True)

print(data)


figure = go.Figure(data=[go.Candlestick(x=data["Date"], 
                                        open=data["Open"], high=data["High"], 
                                        low=data["Low"],close=data["Close"])])

figure.update_layout(title= "Google Stock Price Analysis YOHAN", xaxis_rangeslider_visible=False)


figure = figure.write_html("test4.html")

#save figure to .jpeg
#figure = figure.write_image("reports/figure7.jpeg")
#figure.show()

#another figure with buttons time 
figure = px.line(data, x='Date', y='Close',
                  title='Stock Market Analysis with time period selectors')

#buttons in diagram-figure - period selectors
figure.update_xaxes(
                    rangeselector=dict(
                        buttons=list([
                            dict(count=1, label="1m", step="month", stepmode="backward"),
                            dict(count=6, label="6m", step="month", stepmode="backward"),
                            dict(count=3, label="3m", step="month", stepmode="backward"),
                            dict(count=1, label="1y", step="year", stepmode="backward"),
                            dict(step="all")
                        ])
                    )
)

figure.show()





