# mysqletl
mysql数据迁移示例，练习使用。

非常感谢项目[pyetl](https://github.com/taogeYT/pyetl)的作者

因脚本是专用于迁移MySQL的，为应对某些项目的SX项目经理，用了关键词做字段名称，脚本稍作修改，避免迁移报错。

文件 `$PythonEnv\Lib\site-packages\pydbclib\database.py`

修改了 `_get_insert_sql` 函数(LINE:259)
```
def _get_insert_sql(self, columns):
    return f"insert into {self.name} ({','.join('`' + c + '`' for c in columns)})" \
           f" values ({','.join([':%s' % i for i in columns])})"
```

文件 `pyetl\reader.py`

修改了 `_query_text` 函数(LINE:50)

```
def _query_text(self, columns):
        fields = [f"`{col}` as `{alias}`" for col, alias in columns.items()]
        return " ".join(["select", ",".join(fields), "from ", self.table_name])
```