from typing import List

from data_structures.column import Column
from data_structures.function import Function
from data_structures.table import Table
from data_structures.db_dialect import get_current_dialect


def create_sample_tables() -> List[Table]:
    """Create sample table definitions."""
    t1 = Table(
        name="t1",
        columns=[
            Column("c1", "INT", "numeric", False, "t1"),
            Column("c2", "VARCHAR(255)", "string", False, "t1"),
            Column("c3", "VARCHAR(255)", "string", True, "t1"),
            Column("c4", "INT", "numeric", True, "t1"),
            Column("c5", "DATE", "datetime", False, "t1"),
            Column("c6", "VARCHAR(10)", "string", False, "t1"),
        ],
        primary_key="c1",
        foreign_keys=[],
    )

    t2 = Table(
        name="t2",
        columns=[
            Column("c1", "INT", "numeric", False, "t2"),
            Column("c2", "INT", "numeric", False, "t2"),
            Column("c3", "DECIMAL(10,2)", "numeric", False, "t2"),
            Column("c4", "VARCHAR(50)", "string", False, "t2"),
            Column("c5", "DATE", "datetime", False, "t2"),
            Column("c6", "MEDIUMTEXT", "string", True, "t2"),
            Column("c7", "LONGTEXT", "string", True, "t2"),
            Column("c8", "MEDIUMBLOB", "binary", True, "t2"),
            Column("c9", "LONGBLOB", "binary", True, "t2"),
            Column("c10", "ENUM('value1','value2','value3')", "string", True, "t2"),
            Column("c11", "SET('a','b','c','d')", "string", True, "t2"),
            Column("c12", "BIT(8)", "binary", True, "t2"),
            Column("c13", "DATETIME", "datetime", True, "t2"),
            Column("c14", "FLOAT(8,2)", "numeric", True, "t2"),
            Column("c15", "DOUBLE(12,4)", "numeric", True, "t2"),
            Column("c16", "JSON", "json", True, "t2"),
        ],
        primary_key="c1",
        foreign_keys=[{"column": "c2", "ref_table": "t1", "ref_column": "c1"}],
    )

    t3 = Table(
        name="t3",
        columns=[
            Column("c1", "INT", "numeric", False, "t3"),
            Column("c2", "INT", "numeric", False, "t3"),
            Column("c3", "INT", "numeric", False, "t3"),
            Column("c4", "YEAR", "numeric", False, "t3"),
            Column("c5", "DATETIME", "datetime", True, "t3"),
            Column("c6", "TINYINT", "numeric", True, "t3"),
            Column("c7", "SMALLINT", "numeric", True, "t3"),
            Column("c8", "MEDIUMINT", "numeric", True, "t3"),
            Column("c9", "BIGINT", "numeric", True, "t3"),
            Column("c10", "LONGTEXT", "string", True, "t3"),
            Column(
                "c11",
                "VARCHAR(255)" if get_current_dialect().name == "TIDB" else "GEOMETRY",
                "binary",
                True,
                "t3",
            ),
            Column("c12", "TINYTEXT", "string", True, "t3"),
            Column("c13", "TINYBLOB", "binary", True, "t3"),
            Column("c14", "SET('x','y','z')", "string", True, "t3"),
            Column("c15", "TINYINT(1)", "numeric", True, "t3"),
        ],
        primary_key="c1",
        foreign_keys=[
            {"column": "c2", "ref_table": "t1", "ref_column": "c1"},
            {"column": "c3", "ref_table": "t2", "ref_column": "c1"},
        ],
    )

    return [t1, t2, t3]


def create_sample_functions() -> List[Function]:
    """创建示例函数列表，基于MySQL 8.4聚合函数规范"""
    # 获取当前数据库方言
    from data_structures.db_dialect import DBDialectFactory
    current_dialect = DBDialectFactory.get_current_dialect()
    
    # 根据方言设置LAG和LEAD函数的最大参数个数
    lag_max_params = 3
    lead_max_params = 3
    if current_dialect.name == "MARIADB":
        lag_max_params = 2
        lead_max_params = 2
    
    functions = [
        # 聚合函数 - 基于MySQL 8.4官方文档
        # 基本聚合函数
        Function("COUNT", 1, 1, ["any"], "INT", "aggregate"),
        Function("COUNT_DISTINCT", 1, 1, ["any"], "INT", "aggregate"),
        # SUM返回类型：对精确值参数返回DECIMAL，对近似值参数返回DOUBLE
        Function("SUM", 1, 1, ["numeric"], "numeric", "aggregate"),
        Function("SUM_DISTINCT", 1, 1, ["numeric"], "numeric", "aggregate"),
        # AVG返回类型：对精确值参数返回DECIMAL，对近似值参数返回DOUBLE
        Function("AVG", 1, 1, ["numeric"], "numeric", "aggregate"),
        # MAX/MIN可接受各种类型参数
        Function("MAX", 1, 1, ["any"], "any", "aggregate"),
        Function("MIN", 1, 1, ["any"], "any", "aggregate"),
        
        # 位运算聚合函数 - 支持二进制字符串和数值类型
        Function("BIT_AND", 1, 1, ["numeric"], "numeric", "aggregate"),
        Function("BIT_OR", 1, 1, ["numeric"], "numeric", "aggregate"),
        Function("BIT_XOR", 1, 1, ["numeric"], "numeric", "aggregate"),
        
        # 字符串聚合函数 - GROUP_CONCAT支持多个参数和各种类型
        Function("GROUP_CONCAT", 1, None, ["any"], "string", "aggregate"),
        
        # 统计分析聚合函数 - 对数值参数返回DOUBLE值
        Function("STD", 1, 1, ["numeric"], "DOUBLE", "aggregate"),
        Function("STDDEV", 1, 1, ["numeric"], "DOUBLE", "aggregate"),
        Function("STDDEV_POP", 1, 1, ["numeric"], "DOUBLE", "aggregate"),
        Function("STDDEV_SAMP", 1, 1, ["numeric"], "DOUBLE", "aggregate"),
        Function("VARIANCE", 1, 1, ["numeric"], "DOUBLE", "aggregate"),
        #Function("VAR_POP", 1, 1, ["numeric"], "DOUBLE", "aggregate"),
        Function("VAR_SAMP", 1, 1, ["numeric"], "DOUBLE", "aggregate"),
        
        # JSON聚合函数
        Function("JSON_ARRAYAGG", 1, 1, ["string"], "json", "aggregate"),
        Function("JSON_OBJECTAGG", 2, 2, ["string", "string"], "json", "aggregate"),
        
        # 窗口函数
        #Function("FIRST_VALUE", 1, 1, ["any"], "any", "window"),
        #Function("LAST_VALUE", 1, 1, ["any"], "any", "window"),

        # 标量函数
        # 字符串函数
        Function("CONCAT", 2, None, ["string", "string"], "string", "scalar"),
        Function("CONCAT_WS", 2, None, ["string", "string"], "string", "scalar"),
        Function("SUBSTRING", 2, 3, ["string", "numeric"], "string", "scalar"),
        Function("SUBSTRING_INDEX", 3, 3, ["string", "string", "numeric"], "string", "scalar"),
        Function("LEFT", 2, 2, ["string", "numeric"], "string", "scalar"),
        Function("RIGHT", 2, 2, ["string", "numeric"], "string", "scalar"),
        Function("TRIM", 1, 3, ["string"], "string", "scalar"),
        Function("LTRIM", 1, 1, ["string"], "string", "scalar"),
        Function("RTRIM", 1, 1, ["string"], "string", "scalar"),
        Function("LOWER", 1, 1, ["string"], "string", "scalar"),
        Function("UPPER", 1, 1, ["string"], "string", "scalar"),
        Function("LENGTH", 1, 1, ["string"], "INT", "scalar"),
        Function("CHAR_LENGTH", 1, 1, ["string"], "INT", "scalar"),
        Function("REPLACE", 3, 3, ["string", "string", "string"], "string", "scalar"),
        Function("REPEAT", 2, 2, ["string", "numeric"], "string", "scalar"),
        Function("REVERSE", 1, 1, ["string"], "string", "scalar"),
        #Function("SPLIT_STR", 3, 3, ["string", "string", "numeric"], "string", "scalar"),
        
        # 数学函数
        Function("ABS", 1, 1, ["numeric"], "numeric", "scalar"),
        Function("ROUND", 1, 2, ["numeric"], "numeric", "scalar"),
        Function("CEIL", 1, 1, ["numeric"], "numeric", "scalar"),
        Function("CEILING", 1, 1, ["numeric"], "numeric", "scalar"),
        Function("FLOOR", 1, 1, ["numeric"], "numeric", "scalar"),
        Function("DEGREES", 1, 1, ["numeric"], "numeric", "scalar"),
        Function("MOD", 2, 2, ["numeric", "numeric"], "numeric", "scalar"),
        Function("DIV", 2, 2, ["numeric", "numeric"], "INT", "scalar"),
        Function("POWER", 2, 2, ["numeric", "numeric"], "DOUBLE", "scalar"),
        Function("SQRT", 1, 1, ["numeric"], "DOUBLE", "scalar"),
        Function("LOG", 1, 2, ["numeric"], "DOUBLE", "scalar"),
        Function("LOG10", 1, 1, ["numeric"], "DOUBLE", "scalar"),
        Function("LN", 1, 1, ["numeric"], "DOUBLE", "scalar"),
        Function("EXP", 1, 1, ["numeric"], "DOUBLE", "scalar"),
        Function("PI", 0, 0, [], "DOUBLE", "scalar"),
        
        # 三角函数
        Function("SIN", 1, 1, ["numeric"], "numeric", "scalar"),
        Function("COS", 1, 1, ["numeric"], "numeric", "scalar"),
        Function("TAN", 1, 1, ["numeric"], "numeric", "scalar"),
        Function("ASIN", 1, 1, ["numeric"], "numeric", "scalar"),
        Function("ACOS", 1, 1, ["numeric"], "numeric", "scalar"),
        Function("ATAN", 1, 2, ["numeric"], "numeric", "scalar"),
        Function("ATAN2", 2, 2, ["numeric", "numeric"], "numeric", "scalar"),
        
        # 时间日期函数（仅保留确定性函数）
        Function("YEAR", 1, 1, ["datetime"], "INT", "scalar"),
        Function("MONTH", 1, 1, ["datetime"], "INT", "scalar"),
        Function("DAY", 1, 1, ["datetime"], "INT", "scalar"),
        Function("DAYNAME", 1, 1, ["datetime"], "string", "scalar"),
        Function("DAYOFMONTH", 1, 1, ["datetime"], "INT", "scalar"),
        Function("DAYOFWEEK", 1, 1, ["datetime"], "INT", "scalar"),
        Function("DAYOFYEAR", 1, 1, ["datetime"], "INT", "scalar"),
        Function("HOUR", 1, 1, ["datetime"], "INT", "scalar"),
        Function("MINUTE", 1, 1, ["datetime"], "INT", "scalar"),
        Function("SECOND", 1, 1, ["datetime"], "INT", "scalar"),
        Function("DATE", 1, 1, ["datetime"], "date", "scalar"),
        Function("TIME", 1, 1, ["datetime"], "time", "scalar"),
        #Function("DATETIME", 1, 1, ["numeric"], "datetime", "scalar"),
        Function("DATE_ADD", 3, 3, ["datetime", "string", "numeric"], "datetime", "scalar"),
        Function("DATE_SUB", 3, 3, ["datetime", "string", "numeric"], "datetime", "scalar"),
        Function("ADDDATE", 2, 3, ["datetime", "numeric"], "datetime", "scalar"),
        Function("SUBDATE", 2, 3, ["datetime", "numeric"], "datetime", "scalar"),
        Function("DATEDIFF", 2, 2, ["date", "date"], "INT", "scalar"),
        Function("TIMEDIFF", 2, 2, ["datetime", "datetime"], "time", "scalar"),
        Function("TIMESTAMPDIFF", 3, 3, ["string", "datetime", "datetime"], "INT", "scalar"),
        Function("DATE_FORMAT", 2, 2, ["datetime", "string"], "string", "scalar", format_string_required=True),
        Function("STR_TO_DATE", 2, 2, ["string", "string"], "datetime", "scalar", format_string_required=True),
        
        # 类型转换函数
        Function("CAST", 2, 2, ["any", "string"], "any", "scalar"),
        Function("CONVERT", 2, 2, ["any", "string"], "any", "scalar"),
        
        # 条件函数
        Function("IF", 3, 3, ["any", "any", "any"], "any", "scalar"),
        Function("IFNULL", 2, 2, ["any", "any"], "string", "scalar"),
        Function("NULLIF", 2, 2, ["any", "any"], "string", "scalar"),
        
        # JSON函数
        Function("JSON_ARRAY", 0, None, [], "json", "scalar"),
        Function("JSON_OBJECT", 2, 2, ["string", "any"], "json", "scalar"),
        Function("JSON_EXTRACT", 2, None, ["json", "string"], "json", "scalar"),
        Function("JSON_VALUE", 2, 2, ["json", "string"], "any", "scalar"),
        Function("JSON_SET", 3, None, ["json", "string", "any"], "json", "scalar"),
        Function("JSON_INSERT", 3, None, ["json", "string", "any"], "json", "scalar"),
        Function("JSON_REPLACE", 3, None, ["json", "string", "any"], "json", "scalar"),
        Function("JSON_REMOVE", 2, None, ["json", "string"], "json", "scalar"),
        
        #坐标
        Function("POINT", 2, 2, ["numeric", "numeric"], "binary", "scalar"),
        
        # ST functions
        Function("ST_Area", 1, 1, ["binary"], "DOUBLE", "scalar"),
        Function("ST_AsBinary", 1, 2, ["binary", "numeric"], "string", "scalar"),
        Function("ST_AsGeoJSON", 1, 3, ["binary", "numeric", "numeric"], "string", "scalar"),
        Function("ST_AsText", 1, 2, ["binary", "numeric"], "string", "scalar"),
        Function("ST_AsWKB", 1, 2, ["binary", "numeric"], "string", "scalar"),
        Function("ST_AsWKT", 1, 2, ["binary", "numeric"], "string", "scalar"),
        Function("ST_Buffer", 2, 5, ["binary", "numeric", "string", "string", "string"], "binary", "scalar"),
        Function("ST_Buffer_Strategy", 1, 2, ["string", "numeric"], "string", "scalar"),
        Function("ST_Centroid", 1, 1, ["binary"], "binary", "scalar"),
        Function("ST_Collect", 1, 1, ["binary"], "binary", "aggregate"),
        Function("ST_Contains", 2, 2, ["binary", "binary"], "INT", "scalar"),
        Function("ST_ConvexHull", 1, 1, ["binary"], "binary", "scalar"),
        Function("ST_Crosses", 2, 2, ["binary", "binary"], "INT", "scalar"),
        Function("ST_Difference", 2, 2, ["binary", "binary"], "binary", "scalar"),
        Function("ST_Dimension", 1, 1, ["binary"], "INT", "scalar"),
        Function("ST_Disjoint", 2, 2, ["binary", "binary"], "INT", "scalar"),
        Function("ST_Distance", 2, 3, ["binary", "binary", "string"], "DOUBLE", "scalar"),
        Function("ST_Distance_Sphere", 2, 3, ["binary", "binary", "numeric"], "DOUBLE", "scalar"),
        Function("ST_EndPoint", 1, 1, ["binary"], "binary", "scalar"),
        Function("ST_Envelope", 1, 1, ["binary"], "binary", "scalar"),
        Function("ST_Equals", 2, 2, ["binary", "binary"], "INT", "scalar"),
        Function("ST_ExteriorRing", 1, 1, ["binary"], "binary", "scalar"),
        Function("ST_FrechetDistance", 2, 3, ["binary", "binary", "numeric"], "DOUBLE", "scalar"),
        Function("ST_GeoHash", 3, 3, ["numeric", "numeric", "numeric"], "string", "scalar"),
        Function("ST_GeomCollFromText", 1, 3, ["string", "numeric", "numeric"], "binary", "scalar"),
        Function("ST_GeomCollFromTxt", 1, 3, ["string", "numeric", "numeric"], "binary", "scalar"),
        Function("ST_GeomCollFromWKB", 1, 3, ["binary", "numeric", "numeric"], "binary", "scalar"),
        Function("ST_GeomFromGeoJSON", 1, 3, ["json", "numeric", "numeric"], "binary", "scalar"),
        Function("ST_GeomFromText", 1, 3, ["string", "numeric", "numeric"], "binary", "scalar"),
        Function("ST_GeomFromWKB", 1, 3, ["binary", "numeric", "numeric"], "binary", "scalar"),
        Function("ST_GeometryCollectionFromText", 1, 3, ["string", "numeric", "numeric"], "binary", "scalar"),
        Function("ST_GeometryCollectionFromWKB", 1, 3, ["binary", "numeric", "numeric"], "binary", "scalar"),
        Function("ST_GeometryFromText", 1, 3, ["string", "numeric", "numeric"], "binary", "scalar"),
        Function("ST_GeometryFromWKB", 1, 3, ["binary", "numeric", "numeric"], "binary", "scalar"),
        Function("ST_GeometryN", 2, 2, ["binary", "numeric"], "binary", "scalar"),
        Function("ST_GeometryType", 1, 1, ["binary"], "string", "scalar"),
        Function("ST_HausdorffDistance", 2, 3, ["binary", "binary", "numeric"], "DOUBLE", "scalar"),
        Function("ST_InteriorRingN", 2, 2, ["binary", "numeric"], "binary", "scalar"),
        Function("ST_Intersection", 2, 2, ["binary", "binary"], "binary", "scalar"),
        Function("ST_Intersects", 2, 2, ["binary", "binary"], "INT", "scalar"),
        Function("ST_IsClosed", 1, 1, ["binary"], "BOOLEAN", "scalar"),
        Function("ST_IsEmpty", 1, 1, ["binary"], "INT", "scalar"),
        Function("ST_IsSimple", 1, 1, ["binary"], "INT", "scalar"),
        Function("ST_IsValid", 1, 1, ["binary"], "INT", "scalar"),
        Function("ST_IsValidDetail", 1, 1, ["binary"], "json", "scalar"),
        Function("ST_LatFromGeoHash", 1, 1, ["string"], "DOUBLE", "scalar"),
        Function("ST_Latitude", 1, 2, ["binary", "numeric"], "DOUBLE", "scalar"),
        Function("ST_Length", 1, 2, ["binary", "string"], "DOUBLE", "scalar"),
        Function("ST_LineFromText", 1, 3, ["string", "numeric", "numeric"], "binary", "scalar"),
        Function("ST_LineFromWKB", 1, 3, ["binary", "numeric", "numeric"], "binary", "scalar"),
        Function("ST_LineInterpolatePoint", 2, 2, ["binary", "numeric"], "binary", "scalar"),
        Function("ST_LineInterpolatePoints", 2, 2, ["binary", "numeric"], "binary", "scalar"),
        Function("ST_LineStringFromText", 1, 3, ["string", "numeric", "numeric"], "binary", "scalar"),
        Function("ST_LineStringFromWKB", 1, 3, ["binary", "numeric", "numeric"], "binary", "scalar"),
        Function("ST_LongFromGeoHash", 1, 1, ["string"], "DOUBLE", "scalar"),
        Function("ST_Longitude", 1, 2, ["binary", "numeric"], "DOUBLE", "scalar"),
        Function("ST_MLineFromText", 1, 3, ["string", "numeric", "numeric"], "binary", "scalar"),
        Function("ST_MLineFromWKB", 1, 3, ["binary", "numeric", "numeric"], "binary", "scalar"),
        Function("ST_MPointFromText", 1, 3, ["string", "numeric", "numeric"], "binary", "scalar"),
        Function("ST_MPointFromWKB", 1, 3, ["binary", "numeric", "numeric"], "binary", "scalar"),
        Function("ST_MPolyFromText", 1, 3, ["string", "numeric", "numeric"], "binary", "scalar"),
        Function("ST_MPolyFromWKB", 1, 3, ["binary", "numeric", "numeric"], "binary", "scalar"),
        Function("ST_MakeEnvelope", 2, 2, ["binary", "binary"], "binary", "scalar"),
        Function("ST_MultiLineStringFromText", 1, 3, ["string", "numeric", "numeric"], "binary", "scalar"),
        Function("ST_MultiLineStringFromWKB", 1, 3, ["binary", "numeric", "numeric"], "binary", "scalar"),
        Function("ST_MultiPointFromText", 1, 3, ["string", "numeric", "numeric"], "binary", "scalar"),
        Function("ST_MultiPointFromWKB", 1, 3, ["binary", "numeric", "numeric"], "binary", "scalar"),
        Function("ST_MultiPolygonFromText", 1, 3, ["string", "numeric", "numeric"], "binary", "scalar"),
        Function("ST_MultiPolygonFromWKB", 1, 3, ["binary", "numeric", "numeric"], "binary", "scalar"),
        Function("ST_NumGeometries", 1, 1, ["binary"], "INT", "scalar"),
        Function("ST_NumInteriorRing", 1, 1, ["binary"], "INT", "scalar"),
        Function("ST_NumInteriorRings", 1, 1, ["binary"], "INT", "scalar"),
        Function("ST_NumPoints", 1, 1, ["binary"], "INT", "scalar"),
        Function("ST_Overlaps", 2, 2, ["binary", "binary"], "INT", "scalar"),
        Function("ST_PointAtDistance", 2, 2, ["binary", "numeric"], "binary", "scalar"),
        Function("ST_PointFromGeoHash", 2, 2, ["string", "numeric"], "binary", "scalar"),
        Function("ST_PointFromText", 1, 3, ["string", "numeric", "numeric"], "binary", "scalar"),
        Function("ST_PointFromWKB", 1, 3, ["binary", "numeric", "numeric"], "binary", "scalar"),
        Function("ST_PointN", 2, 2, ["binary", "numeric"], "binary", "scalar"),
        Function("ST_PolyFromText", 1, 3, ["string", "numeric", "numeric"], "binary", "scalar"),
        Function("ST_PolyFromWKB", 1, 3, ["binary", "numeric", "numeric"], "binary", "scalar"),
        Function("ST_PolygonFromText", 1, 2, ["string", "numeric"], "binary", "scalar"),
        Function("ST_PolygonFromWKB", 1, 2, ["binary", "numeric"], "binary", "scalar"),
        Function("ST_SRID", 1, 2, ["binary", "numeric"], "any", "scalar"),
        Function("ST_Simplify", 2, 2, ["binary", "numeric"], "binary", "scalar"),
        Function("ST_StartPoint", 1, 1, ["binary"], "binary", "scalar"),
        Function("ST_SwapXY", 1, 1, ["binary"], "binary", "scalar"),
        Function("ST_SymDifference", 2, 2, ["binary", "binary"], "binary", "scalar"),
        Function("ST_Touches", 2, 2, ["binary", "binary"], "INT", "scalar"),
        Function("ST_Transform", 2, 2, ["binary", "numeric"], "binary", "scalar"),
        Function("ST_Union", 2, 2, ["binary", "binary"], "binary", "scalar"),
        Function("ST_Validate", 1, 1, ["binary"], "string", "scalar"),
        Function("ST_Within", 2, 2, ["binary", "binary"], "INT", "scalar"),
        Function("ST_X", 1, 2, ["binary", "numeric"], "DOUBLE", "scalar"),
        Function("ST_Y", 1, 2, ["binary", "numeric"], "DOUBLE", "scalar"),

        
        # 系统函数（仅保留确定性函数）
        Function("DATABASE", 0, 0, [], "string", "scalar"),
        Function("SCHEMA", 0, 0, [], "string", "scalar"),
        Function("VERSION", 0, 0, [], "string", "scalar"),
        
        # 其他标量函数
        Function("MD5", 1, 1, ["string"], "string", "scalar"),
        Function("SHA1", 1, 1, ["string"], "string", "scalar"),
        Function("SHA2", 2, 2, ["string", "numeric"], "string", "scalar"),
        
        # 窗口函数
        Function("ROW_NUMBER", 0, 0, [], "INT", "window"),
        Function("RANK", 0, 0, [], "INT", "window"),
        Function("DENSE_RANK", 0, 0, [], "INT", "window"),
        Function("NTILE", 1, 1, ["numeric"], "INT", "window"),
        Function("CUME_DIST", 0, 0, [], "DOUBLE", "window"),
        Function("PERCENT_RANK", 0, 0, [], "DOUBLE", "window"),
        Function("LAG", 1, lag_max_params, ["any"], "any", "window"),
        Function("LEAD", 1, lead_max_params, ["any", "numeric", "any"], "any", "window"),
        Function("NTH_VALUE", 2, 2, ["any", "numeric"], "any", "window"),
        Function("FIRST_VALUE", 1, 1, ["any"], "any", "window"),
        Function("LAST_VALUE", 1, 1, ["any"], "any", "window"),
        
    ]
    unsupported_functions_by_dialect = {
        "MYSQL": {"ST_IsValidDetail", "ST_Buffer_Strategy"},
        "MARIADB": {
            "ST_IsValidDetail",
            "ST_Buffer_Strategy",
            "ST_GeomCollFromText",
            "ST_GeomCollFromTxt",
            "ST_HausdorffDistance",
            "ST_Latitude",
            "ST_LineInterpolatePoints",
            "ST_LineInterpolatePoint",
            "ST_MakeEnvelope",
            "ST_NumInteriorRing",
            "ST_NumInteriorRings",
            "ST_PointAtDistance",
            "ST_SwapXY",
            "ST_Transform",
            "ST_Validate",
            "ST_FrechetDistance",
            "ST_FrechetDistance",
            "ST_Longitude",

        },
        "PERCONA":{
            "ST_IsValidDetail",
        },
        "POLARDB": {
            "JSON_VALUE",
            "ST_Collect",
            "ST_FrechetDistance",
            "ST_Buffer_Strategy",
            "ST_AsWKT",
            "ST_AsText",
            "ST_GeomFromText",
            "ST_GeometryCollectionFromText",
            "ST_HausdorffDistance",
            "ST_IsValidDetail",
            "ST_Latitude",
            "ST_LineInterpolatePoint",
            "ST_LineInterpolatePoints",
            "ST_Longitude",
            "ST_PointFromText",
            "ST_PointAtDistance",
            "ST_PolygonFromText",
            "ST_SwapXY",
            "ST_Transform",
        },
        "OCEANBASE": {
            "ST_ConvexHull",
            "ST_Disjoint",
            "ST_Dimension",
            "ST_EndPoint",
            "ST_Envelope",
            "ST_ExteriorRing",
            "ST_Buffer_Strategy",
            "ST_GeomCollFromText",
            "ST_GeomCollFromTxt",
            "ST_GeomCollFromWKB",
            "ST_GeomFromGeoJSON",
            "ST_GeometryCollectionFromText",
            "ST_GeometryCollectionFromWKB",
            "ST_GeometryN",
            "ST_GeometryType",
            "ST_GeoHash",
            "ST_HausdorffDistance",
            "ST_InteriorRingN",
            "ST_Intersection",
            "ST_IsValidDetail",
            "ST_IsEmpty",
            "ST_IsClosed",
            "ST_IsSimple",
            "ST_LatFromGeoHash",
            "ST_LineFromWKB",
            "ST_LineFromText",
            "ST_LineInterpolatePoint",
            "ST_LineInterpolatePoints",
            "ST_LineStringFromText",
            "ST_LineStringFromWKB",
            "ST_LongFromGeoHash",
            "ST_MakeEnvelope",
            "ST_MLineFromText",
            "ST_MLineFromWKB",
            "ST_MPolyFromText",
            "ST_MPolyFromWKB",
            "ST_MPointFromText",
            "ST_MPointFromWKB",
            "ST_MultiLineStringFromText",
            "ST_MultiLineStringFromWKB",
            "ST_MultiPointFromText",
            "ST_MultiPointFromWKB",
            "ST_MultiPolygonFromText",
            "ST_MultiPolygonFromWKB",
            "ST_NumInteriorRing",
            "ST_NumInteriorRings",
            "ST_NumGeometries",
            "ST_NumPoints",
            "ST_PointAtDistance",
            "ST_PointFromGeoHash",
            "ST_PointN",
            "ST_PointFromText",
            "ST_PointFromWKB",
            "ST_PolygonFromWKB",
            "ST_PolygonFromText",
            "ST_PolyFromWKB",
            "ST_PolyFromText",
            "ST_SwapXY",
            "ST_Simplify",
            "ST_StartPoint",
            "ST_Touches",
            "ST_Validate",
            "ST_Collect",
            "ST_FrechetDistance",
        },
        "TIDB": {
            "Point",
            "JSON_VALUE",
        },
    }
    dialect_name = current_dialect.name.upper()
    unsupported_functions = unsupported_functions_by_dialect.get(dialect_name, set())
    if unsupported_functions:
        functions = [f for f in functions if f.name not in unsupported_functions]
    if dialect_name == "TIDB":
        functions = [f for f in functions if not f.name.startswith("ST_")]
    return functions
