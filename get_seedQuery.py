import os
import pymysql
import psycopg2
from data_structures.db_dialect import get_current_dialect
import os

class SeedQueryGenerator:
    def __init__(self, file_path='generated_sql/queries.sql', db_config=None):
        self.file_path = file_path
        # 数据库配置默认值（按方言在connect_db中补齐）
        self.db_config = db_config or {}
        # 不再在初始化时读取所有查询，而是按需读取

    def query_iterator(self):
        """使用生成器逐行读取SQL查询文件，减少内存占用
        
        返回:
        - 生成器: 逐个返回SQL查询（保持与原始get_queries方法一致）
        """
        try:
            # 获取绝对路径
            abs_path = os.path.abspath(self.file_path)

            with open(abs_path, 'r', encoding='utf-8') as f:
                # 逐行读取文件
                for line in f:
                    # 去除行首尾的空白字符
                    sql = line.strip()
                    # 忽略空行
                    if sql:
                        yield sql
        except Exception as e:
            print(f"读取SQL文件时出错: {e}")

    def get_queries_count(self):
        """获取SQL查询的总数，而不将所有查询加载到内存
        
        返回:
        - int: 查询总数
        """
        count = 0
        for _ in self.query_iterator():
            count += 1
        return count

    def connect_db(self):
        """根据当前设置的数据库方言连接对应的数据库
        
        返回:
        - conn: 数据库连接对象
        """
        try:
            # 获取当前的数据库方言
            dialect = get_current_dialect()
            dialect_name = dialect.name.upper()
            
            #print(f"正在连接到{dialect_name}数据库...")
            
            # 根据方言类型创建对应的数据库连接
            if dialect_name in ["MYSQL", "MARIADB", "TIDB", "OCEANBASE","PERCONA", "POLARDB"]:
                # MySQL??????????????????????
                if dialect_name == "MYSQL":
                    defaults = {
                        'host': '127.0.0.1',
                        'user': 'root',
                        'password': '123456',
                        'database': 'test',
                        'port': 13306
                    }
                elif dialect_name == "MARIADB":
                    defaults = {
                        'host': '127.0.0.1',
                        'user': 'root',
                        'password': '123456',
                        'database': 'test',
                        'port': 9901
                    }
                elif dialect_name == "TIDB":
                    defaults = {
                        'host': '127.0.0.1',
                        'user': 'root',
                        'password': '123456',
                        'database': 'test',
                        'port': 4000
                    }
                elif dialect_name == "OCEANBASE":
                    defaults = {
                        'host': '127.0.0.1',
                        'user': 'root',
                        'password': '',
                        'database': 'test',
                        'port': 2881
                    }
                elif dialect_name == "PERCONA":
                    defaults = {
                        'host': '127.0.0.1',
                        'user': 'root',
                        'password': '123456',
                        'database': 'test',
                        'port': 23306
                    }
                elif dialect_name == "POLARDB":
                    defaults = {
                        'host': '127.0.0.1',
                        'user': 'polardbx_root',
                        'password': '123456',
                        'database': 'test',
                        'port': 8527
                    }
                else:
                    defaults = {}

                for key, value in defaults.items():
                    if self.db_config.get(key) in [None, ""]:
                        self.db_config[key] = value

                # ??MySQL/MariaDB/TiDB/OceanBase??
                connection_params = {
                    'host': self.db_config['host'],
                    'user': self.db_config['user'],
                    'password': self.db_config['password'],
                    'database': self.db_config['database'],
                    'port': self.db_config['port'],
                    'charset': 'utf8mb4'
                }
                
                # POLARDB连接使用标准参数，不添加特殊二进制处理
                
                conn = pymysql.connect(**connection_params)
            elif dialect_name == "POSTGRESQL":
                # PostgreSQL数据库配置
                if not self.db_config.get('host'):
                    self.db_config['host'] = '127.0.0.1'
                if not self.db_config.get('user'):
                    self.db_config['user'] = 'postgres'
                if self.db_config.get('password') is None:
                    self.db_config['password'] = 'postgres'
                if not self.db_config.get('database'):
                    self.db_config['database'] = 'test'
                # Avoid carrying MySQL default port into Postgres connections
                if not self.db_config.get('port') or self.db_config.get('port') == 13306:
                    self.db_config['port'] = 5432
                
                conn = psycopg2.connect(
                    host=self.db_config['host'],
                    user=self.db_config['user'],
                    password=self.db_config['password'],
                    dbname=self.db_config['database'],
                    port=self.db_config['port']
                )
            else:
                raise ValueError(f"不支持的数据库方言: {dialect_name}")
            
            #print(f"成功连接到{dialect_name}数据库")
            return conn
        except Exception as e:
            print(f"数据库连接失败: {e}")
            return None

    def execute_query_with_connection(self, query, conn, dialect_name=None):
        """执行SQL查询并返回结果
        
        参数:
        - query: SQL查询语句
        - conn: 已建立好的数据库连接
        - dialect_name: 可选，指定数据库方言
        
        返回:
        - 对于SELECT语句：返回一个包含两个元素的元组 (结果集, 列名列表)
        - 对于其他语句：返回受影响的行数
        - 查询失败：返回None
        """
        if not query or query.strip() == '':
            print("空查询，跳过执行")
            return None
        if conn is None:
            return None
        if not dialect_name:
            dialect = get_current_dialect()
            dialect_name = dialect.name.upper()
        try:
            with conn.cursor() as cursor:
                if dialect_name == "POSTGRESQL":
                    import re
                    pattern = r"TO_CHAR\(\s*'([^']+)'\s*,\s*'([^']+)'\s*\)"
                    def add_type_cast(match):
                        date_str = match.group(1)
                        format_str = match.group(2)
                        return f"TO_CHAR('{date_str}'::DATE, '{format_str}')"
                    query = re.sub(pattern, add_type_cast, query)
                elif dialect_name == "MYSQL":
                    import re
                    from data_structures.db_dialect import MySQLDialect
                    mysql_dialect = MySQLDialect()
                    function_pattern = r"\b([A-Z][A-Z_]+)\s*\("
                    def apply_function_mapping(match):
                        function_name = match.group(1)
                        mapped_name = mysql_dialect.get_function_name(function_name)
                        return f"{mapped_name}("
                    query = re.sub(function_pattern, apply_function_mapping, query)

                cursor.execute(query)
                processed_query = query.strip().upper()
                while processed_query.startswith('('):
                    processed_query = processed_query[1:].strip()
                is_select_query = processed_query.startswith('SELECT') or processed_query.startswith('WITH')
                if is_select_query:
                    result = cursor.fetchall()
                    column_names = [desc[0] for desc in cursor.description] if cursor.description else []
                    return (result, column_names)
                else:
                    conn.commit()
                    return cursor.rowcount
        except Exception as e:
            print(f"查询执行失败: {e}")
            raise

    def execute_query(self, query):
        """执行SQL查询并返回结果
        
        参数:
        - query: SQL查询语句
        
        返回:
        - 对于SELECT语句：返回一个包含两个元素的元组 (结果集, 列名列表)
        - 对于其他语句：返回受影响的行数
        - 查询失败：返回None
        """
        conn = self.connect_db()
        if conn is None:
            return None
        try:
            return self.execute_query_with_connection(query, conn)
        except Exception:
            return None
        finally:
            if conn:
                conn.close()

    def execute_queries(self):
        """执行所有SQL查询（使用生成器减少内存使用）
        """
        for query in self.query_iterator():
            self.execute_query(query)

    def get_seedQuery(self, batch_size=500):
        """获取种子查询（分批处理，减少内存占用）
        
        参数:
        - batch_size: 每批处理的查询数量
        """
        # 获取当前数据库方言
        dialect = get_current_dialect()
        dialect_name = dialect.name.upper()
        
        # 预先创建文件并写入数据库特定的使用语句
        seed_file_path = "./generated_sql/seedQuery.sql"
        with open(seed_file_path, "w", encoding="utf-8") as f:
            if dialect_name != "POSTGRESQL":
                target_db = self.db_config.get("database") or "test"
                f.write(f"USE {target_db};\n")
                f.write(dialect.get_session_settings_sql())
                if not dialect.get_session_settings_sql().endswith("\n"):
                    f.write("\n")
            elif dialect_name == "POSTGRESQL":
                f.write("-- PostgreSQL中没有USE语句，连接数据库通过连接参数指定\n")
            else:
                f.write(f"-- 当前方言: {dialect_name}\n")

        # 获取查询总数
        total_queries = self.get_queries_count()
        print(f"共找到 {total_queries} 个SQL查询:")

        # 统计成功的SELECT查询数量
        success_count = 0
        
        # 分批处理查询
        batch = []
        batch_count = 0
        
        # 使用迭代器逐个处理查询
        for i, sql in enumerate(self.query_iterator(), 1):
            if i % 1000 == 0:  # 每处理1000个查询打印一次进度
                print(f"已处理 {i}/{total_queries} 个查询")
            
            print(f"查询 {i}:")
            # 执行查询并获取结果
            result = self.execute_query(sql)
            
            # 处理结果
            if result is not None:
                # 检查result是否为整数（非SELECT查询的情况）
                if isinstance(result, int):
                    # 非SELECT查询
                    print(f"受影响的行数：{result}")
                else:
                    # SELECT查询，无论结果是否为空都添加到批次
                    batch.append(sql)
                    batch.append("\n")
                    success_count += 1
                    print(f"结果行数：{len(result)}")
            else:
                #print(sql)
                print("查询执行失败")
            
            # 当批次达到指定大小时，写入文件并清空批次
            if len(batch) >= batch_size * 2:  # 每个查询后面有一个换行符，所以乘以2
                with open(seed_file_path, "a", encoding="utf-8") as f:
                    for seed in batch:
                        f.write(seed)
                batch = []  # 清空批次，释放内存
                batch_count += 1
        
        # 处理最后一批
        if batch:
            with open(seed_file_path, "a", encoding="utf-8") as f:
                for seed in batch:
                    f.write(seed)
        
        print(f"\n种子查询生成完成！成功提取了 {success_count} 个有效的SELECT查询")
        print(f"种子查询已保存到: {seed_file_path}")


    
       
