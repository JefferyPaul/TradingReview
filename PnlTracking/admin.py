from django.contrib import admin
from .models import *
from import_export import resources

# Register your models here.
admin.site.register(Comparer)


class ComparerResource(resources.ModelResource):

	class Meta:
		model = Comparer


class FigurePnlResource(resources.ModelResource):

	class Meta:
		model = FigurePnl
