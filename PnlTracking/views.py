from django.shortcuts import render, redirect
from . import models
from django.http import HttpResponse, JsonResponse
import json


def ajax_pnl(request):
	data = {}
	pnl = models.FigurePnl.objects.values()
	data['pnl'] = list(pnl)
	return JsonResponse(data)


def index(request):
	comparers = models.Comparer.objects.all()
	funds = comparers.filter(level='Fund')
	accounts = comparers.filter(level='Account')
	strategies = comparers.filter(level='Strategy')
	traders = comparers.filter(level='Trader')

	return render(request, 'PnlTracking/index.html', locals())


def show(request):
	return render(request, 'PnlTracking/show.html', locals())


def test(request):
	comparers = models.Comparer.objects.all()
	funds = comparers.filter(level='Fund')
	accounts = comparers.filter(level='Account')
	strategies = comparers.filter(level='Strategy')
	traders = comparers.filter(level='Trader')
	pnl = models.FigurePnl.objects.all()
	return render(request, 'PnlTracking/test.html', locals())


def testIndex(request):
	comparers = models.Comparer.objects.all()
	funds = comparers.filter(level='Fund')
	accounts = comparers.filter(level='Account')
	strategies = comparers.filter(level='Strategy')
	traders = comparers.filter(level='Trader')

	return render(request, 'PnlTracking/testIndex.html', locals())
