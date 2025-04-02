import json
import pandas as pd
from tqsdk import TqApi, TqAuth, TqBacktest, TargetPosTask, TqSim
from datetime import date
from tqsdk.tafunc import ma,ema
from tqsdk.exceptions import BacktestFinished
import math
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

#跑了8个标的用了6小时20分钟

def create_backtest_record(R):
  data = {
      "策略-参数1":R["Strategy_name"],
      "策略-参数2":R["param_2"],
      "策略-参数3":R["param_3"],
      "标的":R["标的"],
      "开始时间":R["start_date"],
      "结束时间":R["end_date"],

      "保证金" : R["保证金"],
      "使用金额" : R["保证金"]*10,

      "起始金额" : R["report"]["start_balance"],
      "结束金额" : R["report"]["end_balance"],

      "最大回撤" : R["report"]["max_drawdown"]*R["report"]["start_balance"]/(R["保证金"]*10),
      "盈亏比" : R["report"]["profit_loss_ratio"],
      "胜率" : R["report"]["winning_rate"],

      "收益率" : (R["report"]["end_balance"]-R["report"]["start_balance"])/max(R["保证金"]*10,R["report"]["max_drawdown"]*R["report"]["start_balance"]*2)*100

  }

  df = pd.DataFrame(data, index=[0])
  return df

#计算账户内现金能买多少手合约
def calculate_volume(acc, symbol):
    volume = (acc.get_account().available)/(acc.get_margin(symbol))
    return math.floor(volume)

def calculate_vwma(data, window_size):
    """
    计算成交量加权移动平均线（VWMA）

    :param data: 包含日期、收盘价和成交量的数据框
    :param window_size: 计算VWMA的窗口大小
    :return: 包含原始数据和VWMA的数据框
    """
    # 确保数据框中的价格和成交量列是数值类型
    data = data.copy()
    data['close'] = pd.to_numeric(data['close'])
    data['volume'] = pd.to_numeric(data['volume'])
    # 计算价格与成交量的乘积
    data['价格_成交量乘积'] = data['close'] * data['volume']
    # 计算VWMA
    data['VWMA'] = data['价格_成交量乘积'].rolling(window=window_size).sum() / data['volume'].rolling(window=window_size).sum()
    return data["VWMA"]

class Strategy_XMA:
    def __init__(self,user,short,long,X_MA,k=0.5):
        self.user = user
        self.short = short
        self.long = long
        self.X_MA = X_MA
        self.k = k
        self.result_dict = {"Strategy_name":self.X_MA,
                            "param_2" : str(self.short),
                            "param_3" : str(self.long),
                            "标的":None,
                            "保证金" : None,
                            "start_date":None,"end_date":None,"report":None}
        
    def run_strategy(self,symbol,start_date,end_date):
        self.result_dict["标的"] = symbol
        self.result_dict["start_date"] = start_date
        self.result_dict["end_date"] = end_date
        
        #时间转换
        y,m,d = start_date.split("-")
        y,m,d = int(y),int(m),int(d)
        start_date = date(y,m,d)
 
        e_y,e_m,e_d = end_date.split("-")
        e_y,e_m,e_d = int(e_y),int(e_m),int(e_d)
        end_date = date(e_y,e_m,e_d)
 



        acc = TqSim(1000000)
        api = TqApi(acc,backtest=TqBacktest(start_dt=start_date, end_dt=end_date),auth=TqAuth(self.user["username"], self.user["password"]))
        q = api.get_quote(symbol)
        self.result_dict["保证金"] = q.margin
        
        print("策略开始运行{}-{}-{}-{}".format(self.X_MA,self.short,self.long,symbol))
        data_length =self.long + 3  # k线数据长度
        # "duration_seconds=60"为一分钟线, 日线的duration_seconds参数为: 24*60*60
        klines = api.get_kline_serial(symbol, duration_seconds=60*60*24, data_length=data_length)
        target_pos = TargetPosTask(api, symbol)
        try:
            while True:
                api.wait_update()
                A=0
                if api.is_changing(klines.iloc[-1], "datetime"):  # 产生新k线:重新计算SMA
                    A= 1
                    if self.X_MA == "MA":
                        short_avg = ma(klines["close"], self.short)  # 短周期
                        long_avg = ma(klines["close"],  self.long)  # 长周期
                    elif self.X_MA == "EMA":
                        short_avg = ema(klines["close"], self.short)  # 短周期
                        long_avg = ema(klines["close"],  self.long)  # 长周期
                    elif self.X_MA == "VWMA":
                        short_avg = calculate_vwma(klines, self.short)  # 短周期
                        long_avg = calculate_vwma(klines,  self.long)  # 长周期
                    elif self.X_MA == "MA+kσ":
                        short_avg = ma(klines["close"], self.short)  # 短周期
                        long_avg_top= ma(klines["close"],  self.long) + klines["close"].rolling(window=self.long).std()*0.2  # 长周期上轨道
                        long_avg_bottom = ma(klines["close"],  self.long,) -  klines["close"].rolling(window=self.long).std()*0.2 # 长周期下轨道
                        # 均线下穿，做空
                        if long_avg_bottom.iloc[-3] < short_avg.iloc[-3] and long_avg_bottom.iloc[-2] > short_avg.iloc[-2]:
                            target_pos.set_target_volume(-10)
                        # 均线上穿，做多
                        if short_avg.iloc[-3] < long_avg_top.iloc[-3] and short_avg.iloc[-2] > long_avg_top.iloc[-2]:
                            target_pos.set_target_volume(10)
                        continue
                    else:
                        print("X_MA参数输入错误")
                        break
                if A == 1:
                    # 均线下穿，做空
                    if long_avg.iloc[-3] < short_avg.iloc[-3] and long_avg.iloc[-2] > short_avg.iloc[-2]:
                        target_pos.set_target_volume(-10)
                    # 均线上穿，做多
                    if short_avg.iloc[-3] < long_avg.iloc[-3] and short_avg.iloc[-2] > long_avg.iloc[-2]:
                        target_pos.set_target_volume(10)

        except BacktestFinished as e:
            # 回测结束时会执行这里的代码
            print("回测结束")
            self.result_dict["report"] = acc.tqsdk_stat
            api.close()
            return self.result_dict

            # print(acc.tqsdk_stat) 
             # 回测时间内账户交易信息统计结果，其中包含以下字段
            # init_balance 起始资金
            # balance 结束资金
            # max_drawdown 最大回撤
            # profit_loss_ratio 盈亏额比例
            # winning_rate 胜率
            # ror 收益率
            # annual_yield 年化收益率
            # sharpe_ratio 年化夏普率
            # tqsdk_punchline 天勤点评

def run_backtest(xma, s, l, symbol):
    symbol, s_date, e_date = symbol["symbol"], symbol["start_date"], symbol["end_date"]
    A = Strategy_XMA(user, short=s, long=l, X_MA=xma)
    R = A.run_strategy(symbol, s_date, e_date)
    return create_backtest_record(R)

def main():
    backtest_record_list = []
    with ThreadPoolExecutor() as executor:
        # 使用列表推导式创建一个任务列表
        tasks = [executor.submit(run_backtest, xma, s, l, symbol)
                 for xma in list(strategy_list.values())[:1]
                 for s, l in list(param_list.values())[:1]
                 for symbol in symbols[:1]]

        # 使用tqdm来跟踪任务进度
        for future in tqdm(as_completed(tasks), total=len(tasks)):
            backtest_record_list.append(future.result())

    # 将结果合并为DataFrame并保存为CSV文件
    backtest_record_df = pd.concat(backtest_record_list)
    backtest_record_df.to_csv("策略池_黄金.csv", index=False)



symbols_pool_path = "./策略研究/双均线策略/Symbols_and_Strategy/20250319标的池.json"
strategy_pool_path = "./策略研究/双均线策略/Symbols_and_Strategy/策略池.json"
user_path = "./策略研究/双均线策略/Symbols_and_Strategy/账户.json"
backtest_record_path = "策略池{}.csv".format(date.today().strftime("%Y%m%d"))

with open(user_path,'r',encoding='utf8')as fp:
    json_data = json.load(fp)
    user = json_data['TQ_acc']

with open(symbols_pool_path,'r',encoding='utf8')as fp:
    json_data = json.load(fp)
    symbols = list(json_data['回测池——20250320'].values())

with open(strategy_pool_path,'r',encoding='utf8')as fp:
    json_data = json.load(fp)
    strategy_list = json_data['双均线策略-均线类型']
    param_list = json_data['双均线策略-均线周期']

if __name__ == "__main__":
    main()