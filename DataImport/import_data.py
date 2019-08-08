'''
Somewhere
Is
Wrong

'''

import sys,os
os.environ['DJANGO_SETTINGS_MODULE'] = 'TradingReview.settings'
import django
django.setup()
from import_export import resources
from PnlTracking.models import *
import tablib
import pandas as pd


def import_comparer(path):
	path_file = os.path.join(path, 'comparer.csv')
	if not os.path.isfile(path_file):
		print('not find comparer.csv')
		return

	df = pd.read_csv(path_file, names=['comparer_id', 'level', 'super_comparer'])
	columns = ('comparer_id', 'level', 'super_comparer_id')
	data = df.to_dict('split')['data']
	# data = [tuple(i) for i in data]

	comparer_resource = resources.modelresource_factory(model=Comparer)()
	dataset = tablib.Dataset(
		*data,
		headers=columns  # headers=['comparer_id', 'level', 'super_comparer_id']
	)
	result = comparer_resource.import_data(dataset, dry_run=False)
	print(result.has_errors)


def import_figure_pnl(path):
	path_file = os.path.join(path, 'figurePnl.csv')
	if not os.path.isfile(path_file):
		print('not find figurePnl.csv')
		return

	df = pd.read_csv(path_file, names=['date', 'comparer_id', 'figure_id', 'item', 'pnl'])
	columns = ('date', 'comparer_id', 'figure_id', 'item', 'pnl')
	data = df.to_dict('split')['data']
	# data = [tuple(i) for i in data]

	figure_pnl_resource = resources.modelresource_factory(model=FigurePnl)()
	dataset = tablib.Dataset(
		*data,
		headers=columns      # headers=['figure_id', 'date', 'pnl', 'comparer_id']
	)
	result = figure_pnl_resource.import_data(dataset, dry_run=False)
	print('importing figurePnl: %s' % result)


if __name__ == '__main__':
	path_root = r'F:\TradingReview\DataManager\data_save'
	l_dir = os.listdir(path_root)
	l_dir = [i for i in l_dir if os.path.isdir(os.path.join(path_root, i))]
	l_dir.sort()
	path_dir = os.path.join(path_root, l_dir[-1])
	# import_comparer(path_dir)
	# import_figure_pnl(path_dir)
