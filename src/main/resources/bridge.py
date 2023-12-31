import importlib

from pemja import findClass

_GenericRowData = findClass('org.apache.flink.table.data.GenericRowData')
_RowKind = findClass('org.apache.flink.types.RowKind')
_BinaryStringData = findClass('org.apache.flink.table.data.binary.BinaryStringData')


def _has_module(module_name):
    try:
        module_spec = importlib.util.find_spec(module_name)
        return module_spec is not None
    except ImportError:
        return False


def collect(module, func, ctx, params):
    params = params or {}
    res = getattr(importlib.import_module(module), func)(**params)

    if not _has_module('pandas'):
        raise Exception('pandas is not installed')

    import pandas as pd
    # TODO convert type
    if isinstance(res, pd.DataFrame):
        for tp in res.itertuples(index=False, name=None):
            row = _GenericRowData(_RowKind.INSERT, len(tp))
            for i, v in enumerate(tp):
                row.setField(i, _BinaryStringData(str(v)))
            ctx.collect(row)

    # TODO support other types -- raw spi
