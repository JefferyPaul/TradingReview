# coding=utf-8
import pandas as pd
import numpy as np
import json
import csv
from datetime import *


'''
BUG:
1： 为了将live Night Pnl后移一天，会导致失去第一天的pnl，处理方法如下：
		s_index = s_index[:-1]
		df_pnl = df_pnl[1:]
	这个处理方法在Trader层无可厚非。不过也有更好的处理方法——使用Holiday控制 index Date——不需要丢失第一天的Pnl。
	但由于	Pnl是由Trader层组织起来的，使得这个处理方法而在除Trader层以外的superior层，带来了更大的问题：
	在如Strategy层中，会丢失在strategy中并不是第一天，但在某Trader中是第一天的该Trader 的Pnl。
	这么看这个处理方法不仅不完善，而且是不正确的。
		解决方法：将Live Night Pnl Index平移的处理下放到 Plotting的方法中
	【以解决】 使用Holiday.csv

2： 为了使所有对比图的Pnl起始点均为“0”，在 func：plot_pm_compare_pnl中，有如下处理：	
	pnl_live_cum = df_pnl.cumsum()
	pnl_live_cum -= pnl_live_cum.iloc[0]
		解决方法可能是引入Holiday从而构建完整的 date index。
	【以解决】 使用Holiday.csv

3： 在这个Comparer类中，过分强调Trader的基础性，和不同层之间的层级关系。
		Strategy Account Fund类中由于使用subComparer来组织（和连接）下一级的实例和数据，且数据最终均自Trader实例，
		导致整个结构都特别强调层级关系，其实就是强调上级对下级的 一对多关系，
			所以如果出现上下层是多对多关系的情况，处理结果便会有很多问题和漏洞。
	
	例如Account 和 strategy之间的关系，其实便是多对多的（因为account 其实是与 Trader-Ticker是上下级一对多关系）。
	但是我直接将account和strategy当成上下级关系，
		处理方法是将strategy涉及的account都组合起来形成类似MulAccount的概念，
		这样组合起来的MulAccount和strategy便是一对多的了，如 Prop5的807012.SQniWxNi 策略对应 180380和11180151两个account，
			处理方法是 "prop5"  => "180380 - 11180151" => "807012.SQniWxNi"。
				这样处理能够解决眼前问题，但是可能无法进一步细化和完善。
					解决方法可能是要设Ticker类。但是这样5个类（和实例）之间的关系可能更加复杂。
			【20190722更新】
			目前在comparer类以外通过函数处理这个问题，并能达到上述的效果：
				在使用set_comparer_structure()方法 组织d_funds结构时，加入多Accounts的处理，
					在完成正常方法组织d_funds后，通过combine_accounts方法，
						将同一个fund下，一个strategy对应多个account的情况，组成新的account，
						并在旧account中remove相应strategy。

4： 策略名字。目前显示的名字均为Live db中的名字，可以换为标准的PM名字
'''

'''
settle Pnl实际上是Ticker的属性，完整的关系如下：
Pnl -> Ticker
Ticker -> Trader
Ticker -> Account
Trader -> Strategy
Trader -> Fund (或 strategy -> Fund)
'''

'''



=============================================
Comparer.Pnl: pd.DataFrame index=Date(datetime) columns=[Pnl]
Comparer.PMPnl: pd.DataFrame index=Date columns=[Pnl, MulPnl]
Comparer.Mul: pd.DataFrame index=Date columns=[Mul, InitX, Multiplier]
Comparer.PnlPInitX: pd.DataFrame index=Date columns=[PnlPInitX]
Comparer.PMPnlPInitX: pd.DataFrame index=Date columns=[PnlPInitX]
'''

with open(r"./config.json", encoding='gb2312') as f:
	config = json.loads(f.read())
	path_holiday_file = config.get('holiday_file')
with open(path_holiday_file) as f:
	reader = csv.reader(f)
	l_holiday = list(reader)
holiday = [i[-1] for i in l_holiday]
l_dt_holiday = [datetime.strptime(i, '%Y/%m/%d') for i in holiday]


class Comparer:
	def __init__(self, Id):
		self.Id = Id
		self.Pnl = pd.DataFrame()
		self.PMPnl = pd.DataFrame()
		self.PnlPInitX = pd.DataFrame()
		self.PMPnlPInitX = pd.DataFrame()


# TODO 完善底层 -- Ticker
class Ticker:
	def __init__(self):
		pass


class Trader(Comparer):
	def __init__(self, Id, owned_strategy, owned_account, owned_fund, trader_name, pm_trader_id, is_night=False):
		Comparer.__init__(self, Id)
		self.OwnedStrategy = owned_strategy
		self.OwnedAccount = owned_account
		self.OwnedFund = owned_fund
		self.TraderName = trader_name
		self.is_Night = is_night
		self.PMTraderId = pm_trader_id
		self.Mul = pd.DataFrame()
		self.a = holiday

	def set_live_pnl(self, df):
		# columns = [Date, TraderName, TraderId,
		#           Ticker, StrategyName, Account, FundName,
		#           Pnl, Commission, InitX, Multiplier, Mul]
		df = pd.DataFrame(df)
		df['Date'] = pd.to_datetime(df['Date'], format='%Y%m%d')

		df_pnl = df[['Date', 'Pnl']]
		df_pnl = df_pnl.groupby(by='Date').sum()
		df_pnl = df_pnl.sort_index()
		df_mul = df[['Date', 'Mul', 'InitX', 'Multiplier']]
		df_mul = df_mul.groupby(by='Date').mean()
		df_mul = df_mul.sort_index()

		self.Pnl = df_pnl
		self.Mul = df_mul
		self.PnlPInitX = pd.DataFrame(df_pnl['Pnl'] / df_mul['InitX'], columns=['PnlPInitX'])

		if self.is_Night == 'N':
			self.fix_night_pnl()

	# 对于夜盘策略，PM上的日期是策略运行的日历日期，而live时是“settle date”为交易日日期（中国期货市场从21:00开始算T+1交易日）
	# 所以调整方法为，将live 日期往前调一天
	# 但是由于交易日（需要被调整的live pnl的时间轴）不连续，所以往前调一天不是 date-1
	# 需要使用Holiday.csv
	def fix_night_pnl(self):
		l_pnl_index = self.Pnl.index
		if len(l_pnl_index) <= 1:
			return
		dt_s = min(l_pnl_index) - timedelta(20)
		dt_e = max(l_pnl_index) + timedelta(20)
		l_dt_all = pd.date_range(dt_s, dt_e, freq='D').tolist()
		l_dt_td = [i for i in l_dt_all if i not in l_dt_holiday]

		l_pnl_index_new = []
		# print(l_pnl_index, l_dt_td)
		for dt_i in l_pnl_index:
			dt_new = l_dt_td[l_dt_td.index(dt_i) - 1]
			l_pnl_index_new.append(dt_new)
		self.Pnl.index = l_pnl_index_new
		self.Pnl.index.name = 'Date'
		self.Mul.index = l_pnl_index_new
		self.Mul.index.name = 'Date'
		self.PnlPInitX.index = l_pnl_index_new
		self.PnlPInitX.index.name = 'Date'

		# 旧方法
		# 不使用holiday直接将pnl.index 进行位移，并且移动pmpnl
		# 新做法只移动pnl.index，目标是将pnl.index变为和pm index一致，所以不需要转变pm.index
		# s_index = self.Pnl.index
		# if len(s_index) <= 1:
		# 	self.Pnl = pd.DataFrame()
		# 	self.PMPnl = pd.DataFrame()
		# else:
		# 	s_index = s_index[:-1]
		# 	self.Pnl = self.Pnl[1:]
		# 	self.Pnl.index = s_index
		# 	self.PnlPInitX = self.PnlPInitX[1:]
		#
		# 	self.PMPnl = self.PMPnl[:-1]
		# 	self.PMPnl.index = s_index
		# 	self.PMPnlPInitX = self.PMPnlPInitX[:-1]
		# 	self.PMPnlPInitX.index = s_index

	# 在此方法中，已经按 live Pnl的时间轴去校对 pm Pnl的时间轴了(pd.DataFrame.reindex())，保证在往后的几级中，都不需要校对 live pmPnl的index
	def set_pm_pnl(self, df, init_capital=1000):
		if self.Pnl.empty:
			return

		l_index_pnl = self.Pnl.index.tolist()
		df_mul = self.Mul

		# columns = [Date, PMTraderId, LiveTraderId, Pnl, Commission]
		df = pd.DataFrame(df)
		df['Date'] = pd.to_datetime(df['Date'], format='%Y%m%d')
		df = df[['Date', 'Pnl']]
		df_pm_pnl = df.groupby(by='Date').sum()
		df_pm_pnl = df_pm_pnl.reindex(l_index_pnl)
		# df_pm_pnl = df_pm_pnl.reindex(l_index_pnl, fill_value=0)

		df_pm_pnl = pd.merge(df_pm_pnl, df_mul[['Mul']], how='left', left_index=True, right_index=True)
		df_pm_pnl['MulPnl'] = df_pm_pnl['Pnl'] * df_pm_pnl['Mul'] / init_capital
		self.PMPnl = df_pm_pnl
		self.PMPnlPInitX = pd.DataFrame(self.PMPnl['MulPnl'] / self.Mul['InitX'], columns=['PnlPInitX'])


class SupComparer(Comparer):
	def __init__(self, Id):
		Comparer.__init__(self, Id=Id)
		self.subComparer = []

	def add_sub(self, sub):
		self.subComparer.append(sub)

	def remove_sub(self, sub):
		while sub in self.subComparer:
			self.subComparer.remove(sub)

	def cal_pnl(self):
		if len(self.subComparer) > 0:
			df = pd.concat([i.Pnl for i in self.subComparer])
			if len(df) == 0:
				print('%s has no pnl data' % self.Id)
				return
			df = df.groupby(by='Date').sum()
			self.Pnl = df
		else:
			print(self.Id, '    no subComparer')

	def cal_pm_pnl(self):
		df = pd.concat([i.PMPnl for i in self.subComparer])
		if len(df) == 0:
			print('%s has no pnl data' % self.Id)
			return
		df = df.groupby(by='Date').sum()
		# pd.groupby.sum()  nan相加为0
		self.PMPnl = df


class Strategy(SupComparer):
	def __init__(self, Id):
		SupComparer.__init__(self, Id)

	def cal_pnl_per_initX(self):
		df = pd.concat([i.PnlPInitX for i in self.subComparer])
		if len(df) == 0:
			print('%s has no pnl data' % self.Id)
			return
		df = df.groupby(by='Date').sum()
		self.PnlPInitX = df

	def cal_pm_pnl_per_initX(self):
		df = pd.concat([i.PMPnlPInitX for i in self.subComparer])
		if len(df) == 0:
			print('%s has no pnl data' % self.Id)
			return
		df = df.groupby(by='Date').sum()
		self.PMPnlPInitX = df

	# def cal_pm_pnl(self):
	# 	# 重写，  np.nan加 X 为np.nan
	# 	df2 = pd.DataFrame()
	# 	if len(self.subComparer) == 0:
	# 		print('%s has no pnl data' % self.Id)
	# 		return
	# 	df = pd.concat([i.PMPnl for i in self.subComparer], axis=1)
	# 	if len(df) == 0:
	# 		print('%s has no pnl data' % self.Id)
	# 		return
	# 	# print(type(df['Pnl'].head()))
	# 	df2['Pnl'] = df[['Pnl']].sum(axis=1, skipna=False)
	# 	df2['MulPnl'] = df[['MulPnl']].sum(axis=1, skipna=False)
	# 	# df2.set_index(df.index, inplace=True, drop=True)
	# 	self.PMPnl = df2
	#
	#  艹，想的不够周到。   就算是strategy层的累加，也不能 sum(skipna=True)，
	#  因为有的情况是同strategy下，有的trader是新增加的，有的trader是offline的不再更新的


class Account(SupComparer):
	def __init__(self, Id):
		SupComparer.__init__(self, Id)


class Fund(SupComparer):
	def __init__(self, Id):
		SupComparer.__init__(self, Id)
