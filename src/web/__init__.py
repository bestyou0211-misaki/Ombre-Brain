"""
========================================
web/ — Dashboard / HTTP 路由层（从 server.py 巨石文件拆出，镜像 tools/ 的模块化）
========================================

历史上 server.py 把 93 个 @mcp.custom_route 全平铺在一个 5000 行文件里。
这里按域把路由拆成独立模块，每个模块导出 register(mcp)，server.py 启动时统一装配。
共享依赖（config、cookie 会话鉴权等）放在 web/_shared.py（类比 tools/_runtime.py）。

迁移进行中：每迁出一组路由，就在 register_all 里加一行；server.py 里对应的旧定义删除。

对外暴露：register_all(mcp) —— 注册当前已迁移的所有 web 路由模块。
========================================
"""

from . import auth
from . import tunnel
from . import oauth
from . import dashboard
from . import system
from . import meta
from . import search
from . import plans
from . import letters
from . import hooks
from . import buckets
from . import import_api
from . import github
from . import embedding


def register_all(mcp) -> None:
    """注册所有已迁移到 web/ 的路由模块。后续每迁一个模块加一行。"""
    auth.register(mcp)
    tunnel.register(mcp)
    oauth.register(mcp)
    dashboard.register(mcp)
    system.register(mcp)
    meta.register(mcp)
    search.register(mcp)
    plans.register(mcp)
    letters.register(mcp)
    hooks.register(mcp)
    buckets.register(mcp)
    import_api.register(mcp)
    github.register(mcp)
    embedding.register(mcp)
