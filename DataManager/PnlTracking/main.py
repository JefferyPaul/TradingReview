# coding=utf-8
import os
import json
import pyodbc
from datetime import *
from DataManager.PnlTracking.Comparer import *
import matplotlib.pyplot as plt


# TODO 引入Holiday，制作完整的 TradeDateIndex
# TODO 增加MDD Sharpe等description信息

# Night处理方法更改：
# Night只作为Trader的属性，其他层不设置。
# Night Trader直接调整Pnl和PMPnl
# 其他层直接叠加


def get_trader_id_no_long_short(s):
	return str(s).replace('@Long', '').replace('@Short', '')


def get_mapping():
	# 目标：得到包含所有需要对比的Trader层面的Live和PM对应信息，并作为整个流程的起点。

	# 原mapping文件分为两部分：1：已指定liveTraderId、PMTraderId的，2：没指定TraderId只指定了StrategyName的。
	# 所以先将两部分分开： df_mapping_only_traders, df_mapping_only_strategies

	# get mapping文件
	df_mapping = pd.read_excel(path_mapping)
	df_mapping = df_mapping[~df_mapping['PM'].str.contains('-')]
	for i in l_skip_strategies:
		df_mapping = df_mapping[df_mapping['Live'] != i]
	df_mapping_only_traders = df_mapping.dropna(subset=['TraderPort_Live'])
	df_mapping_only_strategies = df_mapping.loc[
	                             [i for i in df_mapping.index.tolist() if
	                              i not in df_mapping_only_traders.index.tolist()], :]

	# 在settle.db relationship中查找 only_strategy 对应所包含的live_traderName
	l_strategies_name = df_mapping_only_strategies['Live'].tolist()
	sql = '''
		SELECT distinct(Trader), StrategyName
		FROM [%s].[dbo].[SettleRelationships]           
		where StrategyName in ('%s') and date>%s
	''' % (db_name, "', '".join(l_strategies_name), start_date)              #settle
	conn1 = pyodbc.connect(conn_info1)
	df_traders_in_strategies = pd.read_sql(sql, conn1)
	conn1.close()
	df_traders_only_strategies = pd.merge(
		df_traders_in_strategies, df_mapping_only_strategies,
		how='left', left_on='StrategyName', right_on='Live'
	)
	df_traders_only_strategies = df_traders_only_strategies[['PM', 'TraderPort_PM', 'Trader', 'Live', 'Night']]
	df_traders_only_strategies = df_traders_only_strategies.rename(index=str, columns={"Trader":"TraderPort_Live"})

	# 在PM.db查找所有的 strategy - trader关系表 : df_pm_mapping
	l_pm_trader = []
	sql = '''
		SELECT * FROM 
		[Platinum.PM].[dbo].[TraderDbo]
	'''
	conn2 = pyodbc.connect(conn_info2)
	df_pm_mapping = pd.read_sql(sql, conn2)
	conn2.close()
	df_pm_mapping['StrategyId'] = df_pm_mapping['StrategyId'].apply(lambda x:x.split("@")[-1])

	# 通过 TraderPort_Live 及其StrategyName_PM，在df_pm_mapping中查找对应的 TraderPort_PM
	# 最后完成df_mapping_only_strategies 的TraderPort_Live 和 TraderPort_P的查找和匹配
	l_pm_strategy = df_traders_only_strategies['PM'].tolist()
	l_live_trader = df_traders_only_strategies['TraderPort_Live'].tolist()
	for i in range(len(l_live_trader)):
		i_pm_strategy = l_pm_strategy[i]
		i_live_trader = l_live_trader[i]
		if i_live_trader[-5:] == 'Short':
			i_live_trader = i_live_trader[:-6]
		elif i_live_trader[-4:] == 'Long':
			i_live_trader = i_live_trader[:-5]
		l_like_pm_trader = df_pm_mapping.loc[df_pm_mapping['StrategyId'] == i_pm_strategy, 'Id'].tolist()
		l_like_pm_trader2 = [
			j for j in l_like_pm_trader
			if j.split("@")[0] == i_live_trader.split("@")[0] and j[-2:] == i_live_trader[-2:]
		]
		if len(l_like_pm_trader2) > 0:
			l_pm_trader.append(l_like_pm_trader2[0])
			if len(l_like_pm_trader2) > 1:
				print('%s - %s has more than 1 matched pm trader, please check' % (i_pm_strategy, i_live_trader))
		else:
			l_pm_trader.append('')
	df_traders_only_strategies['TraderPort_PM'] = l_pm_trader

	# 合并df_mapping_only_traders df_traders_only_strategies，得到完整的mapping
	df_mapping_all = pd.concat([df_mapping_only_traders, df_traders_only_strategies], sort=True)
	df_mapping_all = df_mapping_all.rename(index=str, columns={
		"Live": "LiveStrategyId",
		"PM": "PMStrategyId",
		"TraderPort_Live": "LiveTraderName",
		"TraderPort_PM": "PMTraderId"
	})
	df_mapping_all["LiveTraderId"] = df_mapping_all["LiveTraderName"].apply(get_trader_id_no_long_short)
	df_mapping_all = df_mapping_all.reset_index(drop=True)
	return df_mapping_all


def get_live_data():
	sql = '''
		SELECT Date, Trader, Ticker, Account,Commission, ClosePnl, PositionPnl, InitX, Multiplier
        FROM [%s].[dbo].[SettleLogs]
        where date>%s and trader in ('%s') and mode=1
	''' % (db_name, start_date, "' ,'".join(l_live_trader_name))   # settle
	conn1 = pyodbc.connect(conn_info1)
	df_pnls = pd.read_sql(sql, conn1)
	conn1.close()

	sql = '''
			SELECT Date, FundName, StrategyName, Trader
	        FROM [%s].[dbo].[SettleRelationships]
	        where date>%s and trader in ('%s')
	''' % (db_name, start_date, "' ,'".join(l_live_trader_name))         # settle
	conn1 = pyodbc.connect(conn_info1)
	df_relationship = pd.read_sql(sql, conn1)
	conn1.close()

	df = pd.merge(
		df_pnls, df_relationship,
		how='left',
		left_on=['Date', 'Trader'], right_on=['Date', 'Trader']
	)
	df['Pnl'] = df['ClosePnl'] + df['PositionPnl'] - df['Commission']
	df['Mul'] = df['InitX'] * df['Multiplier']
	df = df[['Date', 'Trader', 'StrategyName', 'Account', 'FundName', 'Pnl', 'Commission', 'InitX', 'Multiplier', 'Mul']]
	df = pd.merge(
		df, df_mapping[['LiveTraderName', 'LiveTraderId']],
		how='left',
		left_on='Trader',
		right_on='LiveTraderName'
	)
	df = df.rename(index=str, columns={'Trader':'TraderName', 'LiveTraderId':'TraderId'})
	df = df.drop(columns=['LiveTraderName'])
	return df


def get_pm_data():
	# 获取pm_trader_data
	sql = '''
			SELECT Date,TraderId, Pnl, Commission, Capital
	        FROM [Platinum.PM].[dbo].[TraderLogDbo]
	        where date>%s and traderId in ('%s')
	''' % (start_date, "' ,'".join(l_pm_trader_name))
	conn2 = pyodbc.connect(conn_info2)
	df = pd.read_sql(sql, conn2)
	conn2.close()
	df = df[['Date', 'TraderId', 'Pnl', 'Commission']]
	df = pd.merge(df, df_mapping[['LiveTraderId', 'PMTraderId']].drop_duplicates(),
	              how='left', left_on='TraderId', right_on='PMTraderId')
	df = df.drop(columns=['TraderId'])

	# 获取pm_strategy_info  （主要用于获取 Init Capital信息）
	sql = '''
			SELECT [Id],[InitCapital],[OutSampleDate],[Name],[OnlineDate]
	        FROM [Platinum.PM].[dbo].[StrategyDbo]
	        where Name in ('%s')
	''' % ("' ,'".join(l_pm_strategy_id))
	conn2 = pyodbc.connect(conn_info2)
	df_2 = pd.read_sql(sql, conn2)
	conn2.close()
	return df, df_2


def set_traders():
	l_traders = []
	l_trader_id = list(set(df_mapping['LiveTraderId'].tolist()))
	for trader_id in l_trader_id:
		d_trader_info = df_mapping[df_mapping['LiveTraderId'] == trader_id].to_dict('list')
		live_trader_name = d_trader_info['LiveTraderName']
		live_strategy_id = d_trader_info['LiveStrategyId'][0]
		pm_strategy_name = d_trader_info['PMStrategyId'][0]
		pm_trader_id = d_trader_info['PMTraderId'][0]
		is_night = d_trader_info['Night'][0]
		try:
			pm_init_capital = float(df_pm_strategy_info.loc[
				                        df_pm_strategy_info['Name'] == pm_strategy_name,
				                        'InitCapital'
			                        ].tolist()[0]) / 1000000
		except IndexError:
			# print('this strategy has no pm init capital: ', pm_strategy_name)
			pm_init_capital = 1000

		df_live_data = df_live_db_data[df_live_db_data['TraderId'] == trader_id]
		if df_live_data.empty:
			# print('%s find any live data' % trader_id)
			continue
		df_pm_data = df_pm_db_data[df_pm_db_data['LiveTraderId'] == trader_id]
		if df_pm_data.empty:
			# print('%s find any PM data' % trader_id)
			continue

		owned_account = list(set(df_live_data['Account'].tolist()))
		owned_fund = list(set(df_live_data['FundName'].tolist()))[0]

		trader = Trader(
			Id=trader_id,
			trader_name=live_trader_name,
			owned_strategy=live_strategy_id,
			owned_account=owned_account,
			owned_fund=owned_fund,
			is_night=is_night,
			pm_trader_id=pm_trader_id
		)
		trader.set_live_pnl(df_live_data)
		# columns = [Date, TraderName, TraderId,
		#           Ticker, StrategyName, Account, FundName,
		#           Pnl, Commission, InitX, Multiplier, Mul]
		trader.set_pm_pnl(df_pm_data, pm_init_capital)
		l_traders.append(trader)
	return l_traders


"""
	处理多Account的情况
"""
def combine_accounts(d_funds):
	# 0 遍历funds
	for fund_name, fund in d_funds.items():
		# 1 查重， 遍历各account 及其 strategy。
		strategys_accounts = {}
		for acc in fund.subComparer:
			acc_name = acc.Id
			for stra in acc.subComparer:
				stra_name = stra.Id
				if stra_name not in strategys_accounts.keys():
					strategys_accounts[stra_name] = []
				strategys_accounts[stra_name].append(acc_name)

		# 2 对多Account情况进行处理
		for strategy_name, l_accounts in strategys_accounts.items():
			if len(l_accounts) <= 1:
				continue

			# 有多个Account的情况
			# 生成new_account_name 和 new_account
			new_account_name = []
			for i in l_accounts:
				if type(i) == list:
					new_account_name += i
				else:
					new_account_name.append(i)
			fund.add_sub(
				Account(Id=new_account_name)
			)
			new_account = [i for i in fund.subComparer if i.Id == new_account_name][0]

			# 转移strategy
			new_strategy = Strategy(
				Id=strategy_name
			)
			for old_account_name in l_accounts:
				old_account = [i for i in fund.subComparer if i.Id == old_account_name][0]
				old_strategy = [i for i in old_account.subComparer if i.Id == strategy_name][0]

				for i_sub in old_strategy.subComparer:
					new_strategy.add_sub(i_sub)

				old_account.remove_sub(
					old_strategy
				)
			new_account.add_sub(new_strategy)

		# 3 若Fund_Account 的 subComparer 为空，移除
		# for acc in fund.subComparer:
		# 	if len(acc.subComparer) <= 0:
		# 		fund.remove_sub(acc)
		fund.subComparer = [acc for acc in fund.subComparer if len(acc.subComparer) > 0]


def set_comparer_structure():
	"""
	:return: d_funds = { fund_name : Fund }
		Fund.subComparer = [ Account1, Account2 ... ]
		Account.subComparer = [ Strategy1, strategy2 ... ]
		Strategy.subComparer = [ Trader1, Trader2 ... ]
	"""
	# 创建结构
	d_funds = {}
	for trader in l_traders:
		trader_strategy = trader.OwnedStrategy
		trader_account = trader.OwnedAccount
		trader_fund = trader.OwnedFund

		if trader_fund not in d_funds.keys():
			d_funds[trader_fund] = Fund(Id=trader_fund)
		fund = d_funds[trader_fund]

		if trader_account not in [i.Id for i in fund.subComparer]:
			fund.add_sub(
				Account(Id=trader_account)
			)
		account = [i for i in fund.subComparer if i.Id == trader_account][0]

		if trader_strategy not in [i.Id for i in account.subComparer]:
			account.add_sub(
				Strategy(Id=trader_strategy)
			)
		strategy = [i for i in account.subComparer if i.Id == trader_strategy][0]

		strategy.add_sub(
			trader
		)

	# 处理多Account的情况
	if True:
		combine_accounts(d_funds)
	return d_funds


def set_comparer_pnl(comparer):
	# 若存在子对比项（Strategy Account Fund），则处理子对比项
	# 直至 实例为 Trader实例
	if hasattr(comparer, 'subComparer'):
		for sub in comparer.subComparer:
			set_comparer_pnl(sub)

		comparer.cal_pnl()
		comparer.cal_pm_pnl()
		if comparer.__class__.__name__ == 'Strategy':
			comparer.cal_pnl_per_initX()
			comparer.cal_pm_pnl_per_initX()
	else:
		return


def comparer_drawer_respectively(comparer, path_root, sup_comparer_id=''):
	comparer_id = comparer.Id

	# 如果Id 是列表，即有多个Id，则合并
	# 原意是用于处理 单Trader 有 多Account 的情况
	if type(comparer_id) == list:
		comparer_id = ' - '.join(comparer_id)

	# 关系链条，生成 [ Fund Account Strategy Trader] 信息
	if sup_comparer_id:
		chain_relation = '%s*%s' % (sup_comparer_id, comparer_id)
	else:
		chain_relation = comparer_id

	# 是否输出data
	if is_to_csv:
		data = [chain_relation, comparer_id, comparer.__class__.__name__]
		path = os.path.join(path_data_to_csv, 'comparer.csv')
		with open(path, mode='a', encoding='gb2312') as f:
			f.write(','.join(data)+'\n')

	if type(comparer) == Trader:
		path_save = path_root
		plot_pm_compare_pnl(comparer=comparer, save_path_root=path_save, comparer_chain=chain_relation)
	else:
		print('Plotting:  %s' % chain_relation)
		path_save = os.path.join(path_root, str(comparer_id))
		if type(comparer) == Strategy:
			plot_pm_compare_pnl(comparer=comparer, save_path_root=path_save, comparer_chain=chain_relation)
			plot_pm_compare_pnl_per_InitX(comparer=comparer, save_path_root=path_save, comparer_chain=chain_relation)
			plot_sub_compare_pnl(comparer, save_path_root=path_save, comparer_chain=chain_relation)
		elif type(comparer) == Account:
			plot_pm_compare_pnl(comparer=comparer, save_path_root=path_save, comparer_chain=chain_relation)
			plot_sub_compare_pnl(comparer, save_path_root=path_save, comparer_chain=chain_relation)
		elif type(comparer) == Fund:
			plot_pm_compare_pnl(comparer=comparer, save_path_root=path_save, comparer_chain=chain_relation)
			plot_sub_compare_pnl(comparer=comparer, save_path_root=path_save, comparer_chain=chain_relation)
		else:
			print('do not recognize this Comparer : %s' % chain_relation)
			return

		# 遍历sub comparer
		for sub in comparer.subComparer:
			comparer_drawer_respectively(
				sub,
				path_root=path_save,
				sup_comparer_id=chain_relation
			)
		return


def plot_pm_compare_pnl(comparer, save_path_root, comparer_chain):
	comparer_id = comparer.Id
	if type(comparer_id) == list:
		comparer_id = ' - '.join(comparer_id)
	title = comparer_id
	try:
		df_pnl = comparer.Pnl['Pnl'].copy()
		df_pm_pnl = comparer.PMPnl['MulPnl'].copy()
	except:
		print('%s has no data to plot' % comparer_id)
		return

	if comparer.__class__.__name__ != 'Trader':
		save_path = "%s/0-%s-VsPm.png" % (save_path_root, title)
	else:
		save_path = "%s/%s-VsPm.png" % (save_path_root, title)

	figure_width = 20
	figure_height = 8
	label_rotation = 90
	dpi = 60

	attr = df_pnl.index.tolist()
	attr = [i.strftime('%Y%m%d') for i in attr]
	pnl_live_cum = df_pnl.cumsum()
	pnl_live_cum -= pnl_live_cum.iloc[0]

	pnl_pm_cum = df_pm_pnl.cumsum()
	pnl_pm_cum -= pnl_pm_cum.iloc[0]
	pnl_delta_cum = pnl_live_cum - pnl_pm_cum

	fig, ax1 = plt.subplots(figsize=(figure_width, figure_height))
	ax1.plot(attr, pnl_live_cum, label='LivePnl', marker='o', color='orange', markersize=8)
	ax1.plot(attr, pnl_pm_cum, label='PmPnlByInitX', marker='o')
	ax1.plot(attr, pnl_delta_cum, label='delta', marker='o', markersize=4, linestyle='--')
	xlabels = ax1.get_xticklabels()
	for xl in xlabels:
		xl.set_rotation(label_rotation)  # 旋转x轴上的label,以免太密集时有重叠
	ax1.grid(True, linestyle="-.", color="grey", linewidth=.5)
	ax1.set_title(title)
	ax1.set_ylabel('Pnl', fontsize=12)
	ax1.legend()
	if not os.path.isdir(save_path_root):
		os.mkdir(save_path_root)
	plt.savefig(save_path, dpi=dpi)
	plt.clf()
	plt.close()

	# 是否将结果输入至db
	if is_to_csv:
		l_data = []
		# LivePnl
		df_pnl = pd.DataFrame(df_pnl)
		df_pnl.columns = ['pnl']
		df_pnl['date'] = attr
		df_pnl['item'] = 'LivePnl'
		df_pnl['comparer_id'] = comparer_chain
		df_pnl['figure_id'] = "%s-pm" % title
		l_data.append(df_pnl)
		# LivePnl
		df_pm_pnl = pd.DataFrame(df_pm_pnl)
		df_pm_pnl.columns = ['pnl']
		df_pm_pnl['date'] = attr
		df_pm_pnl['item'] = 'PmPnlByInitX'
		df_pm_pnl['comparer_id'] = comparer_chain
		df_pm_pnl['figure_id'] = "%s-pm" % title
		l_data.append(df_pm_pnl)

		data = pd.DataFrame(pd.concat(l_data))
		data.to_csv(os.path.join(path_data_to_csv, 'figurePnl.csv'), index=False, mode='a', header=False,
		            columns=['date', 'comparer_id', 'figure_id', 'item', 'pnl'])


def plot_pm_compare_pnl_per_InitX(comparer, save_path_root, comparer_chain=None):
	comparer_id = comparer.Id
	if type(comparer_id) == list:
		comparer_id = ' - '.join(comparer_id)
	title = comparer_id
	try:
		df_pnl_per_initx = comparer.PnlPInitX['PnlPInitX'].copy()
		df_pm_pnl_per_initx = comparer.PMPnlPInitX['PnlPInitX'].copy()
	except:
		print('%s has no data to plot' % comparer_id)
		return

	if comparer.__class__.__name__ != 'Trader':
		save_path = "%s/0-%s-VsPm_Per_InitX.png" % (save_path_root, title)
	else:
		save_path = "%s/%s-VsPm_Per_InitX.png" % (save_path_root, title)

	figure_width = 20
	figure_height = 8
	label_rotation = 90
	dpi = 60

	attr = df_pnl_per_initx.index.tolist()
	attr = [i.strftime('%Y%m%d') for i in attr]
	pnl_live_cum = df_pnl_per_initx.cumsum()
	pnl_live_cum -= pnl_live_cum.iloc[0]

	pnl_pm_cum = df_pm_pnl_per_initx.cumsum()
	pnl_pm_cum -= pnl_pm_cum.iloc[0]

	fig, ax1 = plt.subplots(figsize=(figure_width, figure_height))
	try:
		ax1.plot(attr, pnl_live_cum, label='LivePnlPerInitX', marker='o', color='orange', markersize=8)
		ax1.plot(attr, pnl_pm_cum, label='PmPnlPerInitX', marker='o')
	except:
		print(len(pnl_live_cum), len(pnl_pm_cum), len(attr))
		return
	xlabels = ax1.get_xticklabels()
	for xl in xlabels:
		xl.set_rotation(label_rotation)  # 旋转x轴上的label,以免太密集时有重叠
	ax1.grid(True, linestyle="-.", color="grey", linewidth=.5)
	ax1.set_title(title)
	ax1.set_ylabel('Pnl', fontsize=12)
	ax1.legend()
	if not os.path.isdir(save_path_root):
		os.mkdir(save_path_root)
	plt.savefig(save_path, dpi=dpi)
	plt.clf()
	plt.close()


def plot_sub_compare_pnl(comparer, save_path_root, comparer_chain):
	figure_width = 20
	figure_height = 8
	label_rotation = 90
	dpi = 60

	# 遍历comparer 的subComparer,获取Id和Pnl （Pnl是index为date的df）
	comparer_id = comparer.Id
	if type(comparer_id) == list:
		comparer_id = ' - '.join(comparer_id)
	if type(comparer_id) == list:
		comparer_id = ' - '.join(comparer_id)
	title = comparer_id
	l_pnl = []
	l_pnl_fixed = []
	try:
		df_strategy_pnl = comparer.Pnl['Pnl'].copy()
	except:
		print('%s has no data to plot' % comparer_id)
		return

	for i in comparer.subComparer:
		i_id = i.Id
		if type(i_id) == list:
			i_id = ' - '.join(i_id)
		if len(i.Pnl) == 0:
			continue
		l_pnl.append({i_id: i.Pnl['Pnl']})

	if comparer.__class__.__name__ != 'Trader':
		save_path = "%s/1-%s-Sub.png" % (save_path_root, title)
	else:
		save_path = "%s/%s-Sub.png" % (save_path_root, title)

	# 初始化fig
	attr_dt = df_strategy_pnl.index.tolist()
	attr = [i.strftime('%Y%m%d') for i in attr_dt]
	fig, ax1 = plt.subplots(figsize=(figure_width, figure_height))
	# comparer的pnl
	df_strategy_pnl_cumsum = df_strategy_pnl.cumsum()
	ax1.plot(attr, df_strategy_pnl_cumsum, label=comparer_id, marker='o', markersize=8, linestyle='--')

	# sub comparer
	for i in l_pnl:
		i_id, i_pnl = list(i.items())[0]
		# 在index以内的，用ffill，其他用nan
		l_index_init = i_pnl.index
		start_date_init_index = min(l_index_init)
		end_date_init_index = max(l_index_init)
		l_index_fix = [dt for dt in attr_dt if (dt >= start_date_init_index) and (dt <= end_date_init_index)]
		i_pnl = i_pnl.reindex(l_index_fix, fill_value=0)
		i_pnl_cumsum = i_pnl.cumsum()
		i_pnl_cumsum = i_pnl_cumsum.reindex(attr_dt)
		ax1.plot(attr, i_pnl_cumsum, label=i_id, marker='o', markersize=4)

		if is_to_csv:
			l_pnl_fixed.append({i_id: i_pnl})

	xlabels = ax1.get_xticklabels()
	for xl in xlabels:
		xl.set_rotation(label_rotation)  # 旋转x轴上的label,以免太密集时有重叠

	ax1.grid(True, linestyle="-.", color="grey", linewidth=.5)
	ax1.set_title(title)
	ax1.set_ylabel('Pnl', fontsize=12)
	ax1.legend()
	if not os.path.isdir(save_path_root):
		os.mkdir(save_path_root)
	plt.savefig(save_path, dpi=dpi)
	plt.clf()
	plt.close()

	# 是否将结果输入至db
	if is_to_csv:
		l_data = []
		# comparer
		df_strategy_pnl = pd.DataFrame(df_strategy_pnl)
		df_strategy_pnl.columns = ['pnl']
		df_strategy_pnl['date'] = [i.strftime('%Y%m%d') for i in df_strategy_pnl.index.tolist()]
		df_strategy_pnl['item'] = comparer_id
		df_strategy_pnl['comparer_id'] = comparer_chain
		df_strategy_pnl['figure_id'] = '%s-sub' % title
		l_data.append(df_strategy_pnl)
		# sub comparer
		for i in l_pnl_fixed:
			i_id, i_pnl = list(i.items())[0]
			df_i_pnl = pd.DataFrame(i_pnl)
			df_i_pnl.columns = ['pnl']
			df_i_pnl['date'] = [i.strftime('%Y%m%d') for i in df_i_pnl.index.tolist()]
			df_i_pnl['item'] = i_id
			df_i_pnl['comparer_id'] = comparer_chain
			df_i_pnl['figure_id'] = '%s-sub' % title
			l_data.append(df_i_pnl)
		data = pd.DataFrame(pd.concat(l_data))
		data.to_csv(os.path.join(path_data_to_csv, 'figurePnl.csv'), index=False, mode='a', header=False,
		            columns=['date', 'comparer_id', 'figure_id', 'item', 'pnl'])


if __name__ == '__main__':
	dt_start = datetime.now()

	# ================================= 参数和基本信息 =================================
	with open(r"./config.json", encoding='gb2312') as f:
		config = json.loads(f.read())
	l_skip_strategies = config.get("skip_strategies")
	dt_path_save = datetime.now().strftime('%Y%m%d %H%M%S')

	# ================================= 循环，不同tracking任务 =================================
	d_tracking_info = config.get("tracking_info")
	for tracking_name, tracking_info in d_tracking_info.items():          # tracking_info
		span = tracking_info.get("span")  # 减去x天
		end_date = (datetime.today().date() + timedelta(days=0)).strftime("%Y%m%d")
		start_date = (datetime.today().date() + timedelta(days=-span)).strftime("%Y%m%d")

		path_mapping = tracking_info.get("path_mapping")
		path_save_root = os.path.join(tracking_info.get("path_save_root"), dt_path_save)

		# LiveTrading settle db
		conn_info1 = 'DRIVER={SQL Server};DATABASE=%s;SERVER=%s;UID=%s;PWD=%s' % (
			tracking_info.get('db').get('1').get('db'),
			tracking_info.get('db').get('1').get('host'),
			tracking_info.get('db').get('1').get('user'),
			tracking_info.get('db').get('1').get('pwd')
		)
		# PM settle db
		conn_info2 = 'DRIVER={SQL Server};DATABASE=%s;SERVER=%s;UID=%s;PWD=%s' % (
			tracking_info.get('db').get('2').get('db'),
			tracking_info.get('db').get('2').get('host'),
			tracking_info.get('db').get('2').get('user'),
			tracking_info.get('db').get('2').get('pwd')
		)
		db_name = tracking_info.get('db').get('1').get('db')

		# ================================= 数据 =================================
		# 数据准备
		print('Getting data,   %s' % datetime.now().strftime('%H:%M:%S'))
		df_mapping = get_mapping()
		# columns: LiveStrategyId, PMStrategyId, LiveTraderName, LiveTraderId, PMTraderId, Night
		l_live_trader_name = list(set(df_mapping['LiveTraderName'].tolist()))
		l_pm_trader_name = list(set(df_mapping['PMTraderId'].tolist()))
		l_pm_strategy_id = list(set(df_mapping['PMStrategyId'].tolist()))

		# 【1】 live_data
		df_live_db_data = get_live_data()
		# columns: Date, TraderName, TraderId, Ticker, StrategyName, Account, FundName, Pnl, Commission, InitX, Multiplier, Mul
		# 【2】 pm_data
		df_pm_db_data, df_pm_strategy_info = get_pm_data()
		# columns: Date, PMTraderId, LiveTraderId, Pnl, Commission

		# 创建Trader实例
		print('Setting Traders,   %s' % datetime.now().strftime('%H:%M:%S'))
		l_traders = set_traders()
		d_funds = set_comparer_structure()
		for fund_name, fund in d_funds.items():
			set_comparer_pnl(fund)

		# ================================= toCsv =================================
		# is_to_db = config.get('save_data').get('to_db').get('is_to_db')
		is_to_csv = tracking_info.get('save_data').get('to_csv').get('is_to_csv')
		path_data_to_csv = ''
		if is_to_csv:
			dt = datetime.now().strftime('%Y%m%d_%H%M%S')
			path_data_to_csv = os.path.join(tracking_info.get('save_data').get('to_csv').get('path'), dt)
			if not os.path.exists(path_data_to_csv):
				os.mkdir(path_data_to_csv)

		# ================================= 画图 =================================
		for fund_name, fund in d_funds.items():
			p = path_save_root
			if not os.path.isdir(p):
				os.mkdir(p)
			comparer_drawer_respectively(fund, path_root=p, sup_comparer_id='')

		dt_end = datetime.now()
		print('=================  Finished %s  %s %s s =================' %
		      (tracking_name, datetime.now().strftime('%H:%M:%S'), (dt_end - dt_start).seconds))

	# ================================= End =================================
	print('================= All Finished !!!!!!   %s =================' % datetime.now().strftime('%H:%M:%S'))
	dt_end = datetime.now()
	print((dt_end - dt_start).seconds, ' s')
