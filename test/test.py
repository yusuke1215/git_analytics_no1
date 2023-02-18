import pandas as pd

path="https://raw.githubusercontent.com/recruit-skillcheck/exam_1442e625fa/main/Q-DataSummarization-Basic/no1/data/accesslog_01.csv?token=GHSAT0AAAAAAB642XC6VJ23CH56GP4C7PRWY7PUE5Q"
df1=pd.read_csv(path)

df2=pd.read_csv("https://raw.githubusercontent.com/recruit-skillcheck/exam_1442e625fa/main/Q-DataSummarization-Basic/no1/data/botlist_01.csv?token=GHSAT0AAAAAAB642XC7ZBF6K4MMDSOLTMWCY7PUFWQ")



df0=df1.groupby("REQUEST_TIME",as_index=False)["ID"].count().rename(columns={"ID":"total"})
df0["REQUEST_TIME"]=df0["REQUEST_TIME"].astype(str).str[:8]
df1["REQUEST_TIME"]=df1["REQUEST_TIME"].astype(str).str[:8]
df2["judge"]="NG"

df3=pd.merge(df1,df2[["BOT_IP_ADDRESS","judge"]],left_on='IP_ADDRESS', right_on='BOT_IP_ADDRESS',how="left").drop(columns="BOT_IP_ADDRESS")
df4=df3.query("judge!='NG'").groupby(["REQUEST_TIME"],as_index=False)["ID"].count().sort_values("REQUEST_TIME",ascending=True)
df5=df4.merge(df0,on=["REQUEST_TIME"])
df5.loc[df5["ID"]/df5["total"]>=0.5,"judge"]="OK"
df6=df5.query("judge=='OK'")[["REQUEST_TIME","ID"]]

df6.to_csv('accesslog_01.answer.csv',index=False,header=False) 
