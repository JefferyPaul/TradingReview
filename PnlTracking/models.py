from django.db import models


class FigurePnl(models.Model):

	figure_id = models.CharField(max_length=256, verbose_name='图片Id', null=True, blank=True)
	comparer = models.ForeignKey('Comparer', on_delete=models.CASCADE, related_name='figure_pnl_of_this_comparer')
	item = models.CharField(max_length=256, null=True, blank=True)
	date = models.DateField()
	pnl = models.FloatField()

	def __str__(self):
		return "%s-%s-%s pnl" % (self.figure_id, self.comparer, self.date)

	class Meta:
		ordering = ["-date", "figure_id", "comparer"]
		# unique_together = (("date", "figure_id", "item"), )
		verbose_name = "每日盈亏对比"
		verbose_name_plural = "每日盈亏对比"


class Comparer(models.Model):

	list_levels = (
		('Fund', '产品'),
		('Account', '账号'),
		('Strategy', '策略'),
		('Trader', '子策略'),
	)

	chain_relation = models.CharField(max_length=255, verbose_name='关系链条', primary_key=True, default='')
	comparer_name = models.CharField(max_length=128, null=True, blank=True)
	level = models.CharField(max_length=64, choices=list_levels, null=True, blank=True)

	def __str__(self):
		return self.chain_relation

	class Meta:
		ordering = ["level", "chain_relation"]


