import sys,os
os.environ['DJANGO_SETTINGS_MODULE'] = 'TradingReview.settings'
import django
django.setup()
from PnlTracking.models import *
import pandas as pd


def import_comparer():
	path_comparer = os.path.join(path_dir, 'comparer.csv')
	if not os.path.isfile(path_comparer):
		print('not find comparer.csv')
		return

	with open(path_comparer) as f:
		BulkList = []
		for line in f:
			line = line.replace('\n', '')
			if len(line) == 0:
				continue
			chain_relation, comparer_name, level = line.split(',')
			if chain_relation:
				comparer = Comparer(
					chain_relation=chain_relation,
					comparer_name=comparer_name,
					level=level
				)
			else:
				print('wrong in import comparer: ', chain_relation)
				continue
			BulkList.append(comparer)
		Comparer.objects.bulk_create(BulkList)


def import_figure_pnl():
	path_figure_pnl = os.path.join(path_dir, 'figurePnl.csv')
	if not os.path.isfile(path_figure_pnl):
		print('not find figurePnl.csv')
		return

	with open(path_figure_pnl) as f:
		BulkList = []
		for line in f:
			line = line.replace('\n', '')
			if len(line) == 0:
				continue
			try:
				date, comparer_id, figure_id, item, pnl = line.split(',')
				date = '%s-%s-%s' % (date[:4], date[4:6], date[-2:])
				pnl = round(float(pnl), 2)
			except:
				continue

			figure_pnl = FigurePnl(
				date=date,
				comparer_id=comparer_id,
				figure_id=figure_id,
				item=item,
				pnl=pnl
			)
			BulkList.append(figure_pnl)
		FigurePnl.objects.bulk_create(BulkList)


if __name__ == '__main__':
	path_root = r'F:\TradingReview\DataManager\data_save'
	l_dir = os.listdir(path_root)
	l_dir = [i for i in l_dir if os.path.isdir(os.path.join(path_root, i))]
	l_dir.sort()
	path_dir = os.path.join(path_root, l_dir[-1])
	# import_comparer()
	import_figure_pnl()
