import pymysql


class ToMySQL:
	def __init__(self, db_info):
		self.db_info = db_info
		self.db = None
		self.cursor = None

	def connect(self):
		self.db = pymysql.connect(**self.db_info)
		self.cursor = self.db.sursor()

	def close(self):
		if self.db:
			self.db.close()

	def insert_comparer(self, data):
		data = ''
		sql = """INSERT INTO tradingreview.pnltracking_comparer
				(id, compare_id, level, super_compare_id)
	             VALUES 
	             %s
	             ;
	             """ % data

		try:
			# 执行sql语句
			self.cursor.execute(sql)
			# 提交到数据库执行
			self.db.commit()
		except:
			# 如果发生错误则回滚
			self.db.rollback()

	def insert_pnl(self, data):
		data = ''
		sql = """INSERT INTO tradingreview.pnltracking_comparer
				(id, figure_id, date, pnl, compare_id)
	             VALUES 
	             %s
	             ;
	             """ % data

		try:
			# 执行sql语句
			self.cursor.execute(sql)
			# 提交到数据库执行
			self.db.commit()
		except:
			# 如果发生错误则回滚
			self.db.rollback()

