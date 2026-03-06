import os
from datetime import datetime
from typing import List

from data_structures.db_dialect import get_current_dialect

def generate_index_sqls(tables, dialect):
    """为表生成索引SQL语句，包括增强的联合索引功能，并将索引信息添加到Table对象中
    
    参数:
    - tables: 表列表
    - dialect: 数据库方言
    
    返回:
    - 索引SQL语句列表
    """
    import random
    import re
    index_sqls = []
    
    def is_text_blob_type(data_type):
        """检查数据类型是否为TEXT或BLOB类型"""
        data_type_lower = data_type.lower()
        # 基础TEXT/BLOB类型
        if data_type_lower in ['text', 'longtext', 'mediumtext', 'blob', 'longblob', 'mediumblob']:
            return True
        # 检查是否包含'text'或'blob'关键字（处理变体）
        if 'text' in data_type_lower or 'blob' in data_type_lower:
            return True
        return False
    
    def get_varchar_length(data_type):
        """从VARCHAR类型定义中提取长度"""
        match = re.search(r'varchar\((\d+)\)', data_type.lower())
        if match:
            return int(match.group(1))
        return 255  # 默认长度
    
    def get_column_with_key_length(col_name, data_type):
        """为TEXT/BLOB类型和长VARCHAR类型的列添加适当的键长度，避免超过255字节限制"""
        data_type_lower = data_type.lower()
        
        # 处理TEXT/BLOB类型
        if is_text_blob_type(data_type):
            # 为TEXT类型添加键长度，使用更小的值以避免超过限制
            return f"{col_name}(50)"
        # 处理长VARCHAR类型
        elif 'varchar' in data_type_lower:
            length = get_varchar_length(data_type)
            # 对于长度超过100的VARCHAR列，添加键长度限制
            if length > 100:
                # 使用较小的键长度，考虑UTF8mb4字符集（每个字符最多4字节）
                # 确保键长度不会超过255字节限制
                key_length = min(63, length)  # 63 * 4 = 252字节，保持在限制内
                return f"{col_name}({key_length})"
        return col_name
    
    # 为每个表生成索引
    for table in tables:
        # 初始化索引列表
        if table.indexes is None:
            table.indexes = []
            
        # 首先为主键创建唯一索引
        if table.primary_key:
            # 获取主键列的信息，检查是否为TEXT/BLOB类型
            pk_col = next((col for col in table.columns if col.name == table.primary_key), None)
            pk_col_with_length = table.primary_key
            if pk_col and is_text_blob_type(pk_col.data_type):
                # 主键不应该是TEXT/BLOB类型，但为了健壮性，我们处理这种情况
                pk_col_with_length = f"{table.primary_key}(100)"
                
            index_name = f"idx_{table.name}_pk"
            pk_index_sql = dialect.get_create_index_sql(
                table_name=table.name,
                index_name=index_name,
                columns=[pk_col_with_length],
                is_unique=True
            )
            index_sqls.append(pk_index_sql)
            # 添加到表对象的索引信息中
            table.add_index(index_name, [table.primary_key], is_primary=True)
        
        # 现在我们将所有非主键列分为两类：
        # 1. 直接索引列 - 非TEXT/BLOB类型
        # 2. 需要键长度的列 - TEXT类型（BLOB仍然排除）
        direct_index_columns = []
        text_index_columns = []
        
        for col in table.columns:
            if col.name == table.primary_key:
                continue
                
            if is_text_blob_type(col.data_type):
                # 只允许TEXT类型（不包括BLOB类型）创建索引
                if 'text' in col.data_type.lower() and 'blob' not in col.data_type.lower():
                    text_index_columns.append(col)
            else:
                direct_index_columns.append(col)
        
        # 合并所有可用的索引列
        all_index_columns = direct_index_columns + text_index_columns
        
        # 只有当有适合索引的非主键列时才处理
        if all_index_columns:
            # 为每个表生成1-2个单列索引
            num_single_indexes = random.randint(1, min(2, len(all_index_columns)))
            
            # 选择列来创建单列索引
            columns_to_index = random.sample(all_index_columns, num_single_indexes)
            
            # 为选择的列创建常规索引
            for col in columns_to_index:
                index_name = f"idx_{table.name}_{col.name}"
                # 获取带键长度的列名（如果需要）
                col_with_length = get_column_with_key_length(col.name, col.data_type)
                index_sql = dialect.get_create_index_sql(
                    table_name=table.name,
                    index_name=index_name,
                    columns=[col_with_length],
                    is_unique=False
                )
                index_sqls.append(index_sql)
                # 添加到表对象的索引信息中（存储原始列名，不包含键长度）
                table.add_index(index_name, [col.name])
            
            # 增强的联合索引生成逻辑
            if len(all_index_columns) >= 2:
                # 确保至少生成一个联合索引的概率增加到80%
                if random.random() < 0.8:
                    # 选择2-3个列创建联合索引
                    num_cols = random.randint(2, min(3, len(all_index_columns)))
                    composite_cols = random.sample(all_index_columns, num_cols)
                    
                    # 获取带键长度的列名列表
                    col_names_with_length = [get_column_with_key_length(col.name, col.data_type) for col in composite_cols]
                    # 存储原始列名用于索引信息
                    original_col_names = [col.name for col in composite_cols]
                    
                    index_name = f"idx_{table.name}_{'_'.join(original_col_names)}"
                    index_sql = dialect.get_create_index_sql(
                        table_name=table.name,
                        index_name=index_name,
                        columns=col_names_with_length,
                        is_unique=False
                    )
                    index_sqls.append(index_sql)
                    # 添加到表对象的索引信息中（存储原始列名）
                    table.add_index(index_name, original_col_names)
                
                # 如果有足够多的列，可能生成第二个不同的联合索引
                if len(all_index_columns) >= 4 and random.random() < 0.5:
                    # 确保选择不同的列组合
                    remaining_cols = [col for col in all_index_columns if col not in columns_to_index[:2]]
                    if len(remaining_cols) >= 2:
                        num_cols = random.randint(2, min(3, len(remaining_cols)))
                        composite_cols = random.sample(remaining_cols, num_cols)
                        
                        # 获取带键长度的列名列表
                        col_names_with_length = [get_column_with_key_length(col.name, col.data_type) for col in composite_cols]
                        # 存储原始列名用于索引信息
                        original_col_names = [col.name for col in composite_cols]
                        
                        index_name = f"idx_{table.name}_{'_'.join(original_col_names)}"
                        index_sql = dialect.get_create_index_sql(
                            table_name=table.name,
                            index_name=index_name,
                            columns=col_names_with_length,
                            is_unique=False
                        )
                        index_sqls.append(index_sql)
                        # 添加到表对象的索引信息中（存储原始列名）
                        table.add_index(index_name, original_col_names)
                
                # 生成一个可能包含主键的联合索引（仅在表有主键时）
                if table.primary_key and len(all_index_columns) >= 1 and random.random() < 0.3:
                    # 选择1-2个非主键列与主键组合
                    num_cols = random.randint(1, min(2, len(all_index_columns)))
                    composite_cols = random.sample(all_index_columns, num_cols)
                    
                    # 获取带键长度的主键列名（如果需要）
                    pk_col = next((col for col in table.columns if col.name == table.primary_key), None)
                    pk_with_length = table.primary_key
                    if pk_col and is_text_blob_type(pk_col.data_type):
                        pk_with_length = f"{table.primary_key}(100)"
                    
                    # 获取带键长度的非主键列名列表
                    non_pk_cols_with_length = [get_column_with_key_length(col.name, col.data_type) for col in composite_cols]
                    # 组合列（主键通常放在前面）
                    col_names_with_length = [pk_with_length] + non_pk_cols_with_length
                    # 存储原始列名用于索引信息
                    original_col_names = [table.primary_key] + [col.name for col in composite_cols]
                    
                    index_name = f"idx_{table.name}_pk_{'_'.join([col.name for col in composite_cols])}"
                    index_sql = dialect.get_create_index_sql(
                        table_name=table.name,
                        index_name=index_name,
                        columns=col_names_with_length,
                        is_unique=True  # 包含主键的索引自然是唯一的
                    )
                    index_sqls.append(index_sql)
                    # 添加到表对象的索引信息中（存储原始列名）
                    table.add_index(index_name, original_col_names)
    
    return index_sqls


def save_sql_to_file(sql: str, output_dir: str = "generated_sql", file_type: str = "all", mode: str = "w") -> str:
    """将生成的SQL保存到文件
    
    参数:
    - sql: SQL语句
    - output_dir: 输出目录
    - file_type: 文件类型 ("schema", "query", or "all")
    - mode: 写入模式 ("w"表示覆盖，"a"表示追加)
    """
    # 创建输出目录
    os.makedirs(output_dir, exist_ok=True)

    # 根据文件类型生成文件名
    if file_type == "schema":
        filename = "schema.sql"
    elif file_type == "query":
        filename = "queries.sql"
    else:
        filename = "seedquery.sql"
    
    filepath = os.path.join(output_dir, filename)

    # 获取当前数据库方言
    from data_structures.db_dialect import get_current_dialect
    dialect = get_current_dialect()

    # 写入文件
    with open(filepath, mode, encoding="utf-8") as f:
        if mode == "w":  # 只有在覆盖模式下才写入数据库设置
            if file_type == "schema":
                # 使用方言的方法生成数据库操作语句
                drop_db_sql = dialect.get_drop_database_sql("test")
                create_db_sql = dialect.get_create_database_sql("test")
                f.write(drop_db_sql)
                if drop_db_sql and not drop_db_sql.endswith("\n"):
                    f.write("\n")
                f.write(create_db_sql)
                if create_db_sql and not create_db_sql.endswith("\n"):
                    f.write("\n")
            
            # 使用方言的方法生成使用数据库的语句
            use_db_sql = dialect.get_use_database_sql("test")
            set_session_sql = dialect.get_session_settings_sql()
            if set_session_sql:
                f.write(set_session_sql)
                if not set_session_sql.endswith("\n"):
                    f.write("\n")
            if use_db_sql:
                f.write(use_db_sql)
                if not use_db_sql.endswith("\n"):
                    f.write("\n")
            else:
                # 如果没有USE语句（如PostgreSQL），添加注释说明连接方式
                f.write("-- PostgreSQL中没有USE语句，连接数据库通过连接参数指定\n")
        f.write(sql)

    return filepath


from typing import Optional, Dict
# ------------------------------
# 主函数
# ------------------------------

