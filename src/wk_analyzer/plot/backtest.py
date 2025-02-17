from pyecharts.charts import Line, Grid
import pyecharts.options as opt


def plot_net_value(net_value):
    c = (
        Line(init_opts=opt.InitOpts(
            # width = "1500px",
            # height= "500px",
        ))
        .add_xaxis(net_value.index.tolist())
        .set_global_opts(
            title_opts=opt.TitleOpts(title="净值曲线"),
            legend_opts=opt.LegendOpts(
                pos_right=0,
                orient='vertical'
                # pos_left='10%',
                # type_='scroll',
            ),
            tooltip_opts=opt.TooltipOpts(
                trigger='axis',
                # position=['15%','20%']
            ),
            yaxis_opts=opt.AxisOpts(
                is_scale=True,
                boundary_gap=True,
                name='净值',
                splitline_opts=opt.SplitLineOpts(is_show=True),
            ),
            xaxis_opts=opt.AxisOpts(
                name='日期',
                axistick_opts=opt.AxisTickOpts(is_align_with_label=True)
            ),

            datazoom_opts=[
                opt.DataZoomOpts(
                    range_start=0,
                    range_end=100
                )
            ]
        )
    )
    strategy_name = net_value.columns[0]
    for col in net_value.columns:
        c = c.add_yaxis(
            series_name=col,
            y_axis=net_value[col],
            is_symbol_show=False,
            # is_selected=(col == strategy_name),
            symbol_size=0,
            is_hover_animation=False
        )
    g = Grid(init_opts=opt.InitOpts(
        width="1100px",
        height="500px",
    ))
    g.add(c, grid_opts=opt.GridOpts(width='75%'))
    return g


def plot_drawback(drawback_df):
    c = (
        Line(init_opts=opt.InitOpts(
            # width = "1500px",
            # height= "500px",
        ))
        .add_xaxis(drawback_df.index.tolist())
        .set_global_opts(
            title_opts=opt.TitleOpts(title="回撤详情"),
            legend_opts=opt.LegendOpts(
                pos_right=0,
                orient='vertical'
                # pos_left='10%',
                # type_='scroll',
            ),
            tooltip_opts=opt.TooltipOpts(
                trigger='axis',
                # position=['15%','20%']
            ),
            yaxis_opts=opt.AxisOpts(
                is_scale=True,
                boundary_gap=True,
                name='回撤',
                splitline_opts=opt.SplitLineOpts(is_show=True),
            ),
            xaxis_opts=opt.AxisOpts(
                name='日期',
                axistick_opts=opt.AxisTickOpts(is_align_with_label=True)
            ),

            datazoom_opts=[
                opt.DataZoomOpts(
                    range_start=0,
                    range_end=100
                )
            ]
        )
    )
    strategy_name = drawback_df.columns[0]
    for col in drawback_df.columns:
        c = c.add_yaxis(
            series_name=col,
            y_axis=drawback_df[col],
            is_symbol_show=False,
            is_selected=(col == strategy_name),
            symbol_size=0,
            is_hover_animation=False,
            areastyle_opts=opt.AreaStyleOpts(
                opacity=0.3,
            ),
        )
    g = Grid(init_opts=opt.InitOpts(
        width="1100px",
        height="500px",
    ))
    g.add(c, grid_opts=opt.GridOpts(width='73%'))
    return g
