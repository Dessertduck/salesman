# -*- coding: utf-8 -*-
import sys
import os
import traceback
import pandas as pd
import pymysql
import warnings
from datetime import datetime

warnings.filterwarnings('ignore')

# ────────────────────────────────────────────────
# 1. 数据库配置
# ────────────────────────────────────────────────
DB_CONFIG = {
    "user": "lysjfx02",
    "password": "VpT$jp&im5!G",
    "host": "rr-uf62p8zih4j2n32t5qo.mysql.rds.aliyuncs.com",
    "port": 3306,
    "database": "yzl",
    "charset": "utf8mb4",
    "cursorclass": pymysql.cursors.DictCursor,
}

# ────────────────────────────────────────────────
# 2. 通用查询函数
# ────────────────────────────────────────────────
def query_to_df(sql: str, df_name: str = "data_df") -> pd.DataFrame:
    try:
        with pymysql.connect(**DB_CONFIG) as conn, conn.cursor() as cursor:
            cursor.execute(sql)
            data = cursor.fetchall()
            df = pd.DataFrame(data)
            print(f"✅ {df_name} 查询成功，共获取 {len(df)} 条数据")
            return df
    except Exception as e:
        print(f"❌ {df_name} 执行失败：{e}")
        return pd.DataFrame()

# ────────────────────────────────────────────────
# 3. SQL 语句 (优化了异常退租逻辑)
# ────────────────────────────────────────────────
SQL_RECOMMEND = """
WITH base AS (
  SELECT
    r.order_num                               AS 订单编号,
    r.openid                                  AS 推荐官openid,
    r.invitee_openid                          AS 被推荐人openid,
    yu.nick_name                              AS 名字,
    yu.phone_num                              AS 电话,
    recommender_u.nick_name                   AS 推荐官姓名,
    recommender_u.phone_num                   AS 推荐官手机号,
    rc.city_name                              AS 推荐官城市,
    yr.create_time                            AS 推荐官创建时间, 
    r.biz_manager                             AS 业务员openid,
    biz_u.nick_name                           AS 业务员姓名,
    CASE
      WHEN c.city_name = '台州市' THEN '温州市'
      WHEN c.city_name = '鄂州市'  THEN '武汉市'
      ELSE COALESCE(c.city_name, '')
    END                                      AS 费用归属城市,
    ROUND(r.order_amount / 100, 2)           AS 订单金额,
    ROUND(r.reward / 100, 2)                  AS 收益,
    r.create_time                             AS 下单时间,
    r.order_type                              AS 原始订单类型,
    CASE
      WHEN r.order_type = 'COMMON'        THEN '有效拉新'
      WHEN r.order_type = 'RENEWAL'       THEN '续租'
      WHEN r.order_type = 'order_reward'  THEN '续租'
      WHEN r.order_type = 'order_add'     THEN '特殊补单-首单'
      ELSE r.order_type
    END                                      AS 计费类型,
    CASE
      WHEN r.channel_level = 'standard' THEN '标准'
      WHEN r.channel_level = 'gold'     THEN '金牌'
      WHEN r.channel_level = 'diamond'  THEN '钻石'
      ELSE r.channel_level
    END                                      AS 渠道等级,
    CASE
      WHEN r.record_type = 'master'           THEN '主记录'
      WHEN r.record_type = 'unbind_rentals' THEN '退租'
      WHEN r.record_type = 'adjustment'      THEN '调整单'
      WHEN r.record_type = 'forfeiture'      THEN '作废'
      ELSE r.record_type
    END                                      AS 记录类型,
    r.status                                  AS 状态,
    COALESCE(r.adjust_reasons, '无')         AS 调整说明
  FROM yzl_recommender_reward r
  LEFT JOIN yzl_city_setting c    ON r.reward_city_id = c.id
  LEFT JOIN yzl_user yu            ON r.invitee_openid = yu.openid
  LEFT JOIN yzl_user recommender_u ON r.openid = recommender_u.openid
  LEFT JOIN yzl_recommender yr    ON r.openid = yr.openid 
  LEFT JOIN yzl_city_setting rc   ON yr.city_id = rc.id
  LEFT JOIN yzl_user biz_u         ON r.biz_manager = biz_u.openid
  WHERE r.is_deleted = 0
    AND r.status != 'deleted'
),
non_special_flagged AS (
  SELECT b.*,
    CASE
      WHEN COUNT(*) OVER (PARTITION BY b.订单编号) = 1 THEN 0
      WHEN COUNT(*) OVER (PARTITION BY b.订单编号) > 1
           AND b.记录类型 = '调整单'
           AND NOT (b.调整说明 LIKE '%退租%' OR b.调整说明 LIKE '%租金退款%' OR b.调整说明 LIKE '%老用户%')
      THEN 0 ELSE 1
    END AS 是否异常退租
  FROM base b
  WHERE b.订单编号 NOT IN ('1988254627647758336','BRO1991160250767929344')
),
special_flagged AS (
  SELECT t.订单编号, t.推荐官openid, t.被推荐人openid, t.名字, t.电话,
    t.推荐官姓名, t.推荐官手机号, t.推荐官城市, t.推荐官创建时间, t.业务员openid, t.业务员姓名,
    t.费用归属城市, t.订单金额, t.收益, t.下单时间, t.原始订单类型, t.计费类型,
    t.渠道等级, t.记录类型, t.状态, t.调整说明, 0 AS 是否异常退租
  FROM (
    SELECT b.*, ROW_NUMBER() OVER (PARTITION BY b.订单编号 ORDER BY b.下单时间 DESC) AS rn
    FROM base b
    WHERE b.订单编号 IN ('1988254627647758336','BRO1991160250767929344')
  ) t WHERE t.rn = 1
),
final_detail AS (
  SELECT * FROM non_special_flagged UNION ALL SELECT * FROM special_flagged
)
SELECT 订单编号, 推荐官openid, 被推荐人openid, 名字, 电话,
  推荐官姓名, 推荐官手机号, 推荐官城市, 推荐官创建时间, 业务员openid, 业务员姓名,
  费用归属城市, 订单金额, 收益, 下单时间, 计费类型, 渠道等级, 记录类型, 状态
FROM final_detail WHERE 是否异常退租 = 0 ORDER BY 下单时间;
"""

# ────────────────────────────────────────────────
# 4. 数据处理 (核心逻辑及列顺序调整)
# ────────────────────────────────────────────────
def process_data(recommend_df: pd.DataFrame):
    today = pd.Timestamp.now().normalize()
    curr_month_start = today.replace(day=1)
    yesterday_start  = today - pd.Timedelta(days=1)
    yesterday_end    = today

    df = recommend_df.copy()
    df = df[~df['推荐官城市'].isin(['合肥市', '金华市'])]
    df['下单时间'] = pd.to_datetime(df['下单时间'])
    df['推荐官创建时间'] = pd.to_datetime(df['推荐官创建时间'])

    target_cols = ['推荐官人数', '拉新人数', '活跃推荐官人数']

    def calc_stats(data, start, end):
        mask = (data['下单时间'] >= start) & (data['下单时间'] < end)
        period = data[mask].copy()
        if period.empty: return pd.DataFrame(columns=target_cols)
        
        valid = period[period['计费类型'] == '有效拉新']
        total = period.groupby('业务员姓名')['推荐官openid'].nunique()
        pulls = valid.groupby('业务员姓名')['被推荐人openid'].nunique()
        inv = valid.groupby(['业务员姓名', '推荐官openid'])['被推荐人openid'].nunique().reset_index()
        active = inv[inv['被推荐人openid'] > 3].groupby('业务员姓名')['推荐官openid'].count()
        
        stats = pd.DataFrame(index=total.index)
        stats['推荐官人数'] = total
        stats['拉新人数'] = pulls
        stats['活跃推荐官人数'] = active
        return stats.fillna(0).astype(int)

    stats_yd = calc_stats(df, yesterday_start, yesterday_end).add_prefix('昨日')
    stats_mtd = calc_stats(df, curr_month_start, today).add_prefix('本月至今')

    city_map = df.groupby('业务员姓名')['推荐官城市'].agg(
        lambda x: x.mode().iloc[0] if not x.mode().empty else '未知'
    ).reset_index().rename(columns={'推荐官城市': '城市'})

    salesman_final = city_map.merge(stats_yd, on='业务员姓名', how='left').merge(stats_mtd, on='业务员姓名', how='left').fillna(0)

    # 保底补全缺失列
    expected_cols = ['昨日推荐官人数', '昨日拉新人数', '昨日活跃推荐官人数', '本月至今推荐官人数', '本月至今拉新人数', '本月至今活跃推荐官人数']
    for col in expected_cols:
        if col not in salesman_final.columns: salesman_final[col] = 0

    # 1. 城市汇总表顺序调整
    city_summary = salesman_final.groupby('城市').agg({
        '业务员姓名': 'nunique',
        '昨日推荐官人数': 'sum',
        '本月至今推荐官人数': 'sum',
        '昨日活跃推荐官人数': 'sum',
        '本月至今活跃推荐官人数': 'sum',
        '昨日拉新人数': 'sum',
        '本月至今拉新人数': 'sum',
    }).reset_index().rename(columns={'业务员姓名': '业务员数量'})
    
    city_summary['月净增活跃推荐官人数'] = city_summary['本月至今活跃推荐官人数'] - city_summary['昨日活跃推荐官人数']
    
    final_city_report = city_summary[[
        '城市', '业务员数量', '昨日推荐官人数', '本月至今推荐官人数', 
        '昨日活跃推荐官人数', '本月至今活跃推荐官人数', '月净增活跃推荐官人数', 
        '昨日拉新人数', '本月至今拉新人数'
    ]].sort_values('本月至今拉新人数', ascending=False)

    # 2. 业务员明细表顺序调整
    TARGET_SALESMEN = ['陈浩', '方宇帆', '罗俊', '周金', '何五霞', '鲁冰煜', '李前同', '杨成文', '朱泓', '郭娟', '胡鹏', '刘海', '罗春梅', '沙雪梅', '吴磊', '臧倩', '张宏珠', '张徐月', '姬凯', '田凯特', '王超', '张晓东', '岑赞锁', '陈斌', '高林', '王文波', '朱寅君', '吝世井', '南通曹国庆', '陈海欣', '马裕纯', '叶志洋', '尹志鹏', '岳越-南京', '张小龙', '周文龙', '15387063275', '黄玉松', '简永富', '李正茂', '王学成', '张中梁', '姜少锋', '吴国翠', '张宸']
    
    result_final = salesman_final[salesman_final['业务员姓名'].isin(TARGET_SALESMEN)].copy()
    result_final['月净增活跃推荐官人数'] = result_final['本月至今活跃推荐官人数'] - result_final['昨日活跃推荐官人数']
    
    result_final = result_final[[
        '城市', '业务员姓名', '昨日推荐官人数', '本月至今推荐官人数', 
        '昨日活跃推荐官人数', '本月至今活跃推荐官人数', '月净增活跃推荐官人数', 
        '昨日拉新人数', '本月至今拉新人数'
    ]].sort_values(['城市', '本月至今拉新人数'], ascending=[True, False])

    # 3. 推荐官明细表
    valid_df = df[df['计费类型'] == '有效拉新'].copy()
    rec_stats = valid_df.groupby(['推荐官openid', '推荐官姓名', '业务员姓名', '推荐官城市', '推荐官创建时间']).agg(
        累计拉新=('被推荐人openid', 'nunique'),
        当月拉新=('被推荐人openid', lambda x: x[valid_df.loc[x.index, '下单时间'] >= curr_month_start].nunique()),
        昨日拉新=('被推荐人openid', lambda x: x[(valid_df.loc[x.index, '下单时间'] >= yesterday_start) & (valid_df.loc[x.index, '下单时间'] < yesterday_end)].nunique())
    ).reset_index()

    recommender_report = rec_stats[rec_stats['当月拉新'] > 0].copy().rename(columns={
        '推荐官城市': '城市', '推荐官姓名': '推荐官', '业务员姓名': '业务员', '推荐官创建时间': '创建时间'
    })
    recommender_report = recommender_report[['城市', '推荐官', '业务员', '创建时间', '当月拉新', '昨日拉新', '累计拉新']]
    recommender_report = recommender_report.sort_values(['城市', '当月拉新'], ascending=[True, False]).reset_index(drop=True)

    return final_city_report, result_final, recommender_report

# ────────────────────────────────────────────────
# 5. 美化输出 Excel (确保格式不交叉)
# ────────────────────────────────────────────────
def df_to_beautiful_excel(city_summary, result_final, recommender_report, out_path):
    with pd.ExcelWriter(out_path, engine='xlsxwriter') as writer:
        wb = writer.book
        sht = wb.add_worksheet('监控汇总报表')
        FONT = 'Microsoft YaHei'

        # 定义独立格式，防止互相污染
        fmt_base = {'font_name': FONT, 'font_size': 10, 'valign': 'vcenter', 'align': 'center', 'border': 1}
        cell_normal = wb.add_format(fmt_base)
        cell_int = wb.add_format({**fmt_base, 'num_format': '0'})
        cell_date = wb.add_format({**fmt_base, 'num_format': 'yyyy-mm-dd'})
        
        fmt_zebra = {**fmt_base, 'bg_color': '#F2F2F2'}
        zebra_normal = wb.add_format(fmt_zebra)
        zebra_int = wb.add_format({**fmt_zebra, 'num_format': '0'})
        zebra_date = wb.add_format({**fmt_zebra, 'num_format': 'yyyy-mm-dd'})

        hdr_fmt = wb.add_format({'bold': True, 'bg_color': '#4F81BD', 'font_color': 'white', 'font_name': FONT, 'font_size': 10, 'align': 'center', 'valign': 'vcenter', 'border': 1, 'text_wrap': True})
        title_fmt = wb.add_format({'bold': True, 'font_name': FONT, 'font_size': 14, 'align': 'left', 'valign': 'vcenter'})

        def write_table(df, title, start_row, start_col):
            sht.merge_range(start_row, start_col, start_row, start_col + len(df.columns) - 1, title, title_fmt)
            for ci, col in enumerate(df.columns):
                sht.write(start_row + 1, start_col + ci, col, hdr_fmt)
            
            for ri, row in enumerate(df.values):
                row_idx = start_row + 2 + ri
                is_zebra = (ri % 2 == 1)
                for ci, val in enumerate(row):
                    col_name = df.columns[ci]
                    # 严格判定列格式
                    if '时间' in col_name or isinstance(val, (datetime, pd.Timestamp)):
                        f = zebra_date if is_zebra else cell_date
                    elif isinstance(val, (int, float, complex)) and '编号' not in col_name and 'openid' not in col_name:
                        f = zebra_int if is_zebra else cell_int
                    else:
                        f = zebra_normal if is_zebra else cell_normal
                    sht.write(row_idx, start_col + ci, val, f)

        # 布局
        write_table(city_summary, '1. 城市业务员汇总', 0, 0)
        write_table(result_final, '2. 业务员核心指标明细', len(city_summary) + 4, 0)
        write_table(recommender_report, '3. 推荐官当月拉新明细', 0, len(city_summary.columns) + 2)

        sht.set_column(0, 50, 12)
        sht.freeze_panes(1, 1)

# ────────────────────────────────────────────────
# 6. 主流程
# ────────────────────────────────────────────────
def main():
    if getattr(sys, 'frozen', False): base_dir = os.path.dirname(sys.executable)
    else: base_dir = os.path.dirname(os.path.abspath(__file__))
    out_path = os.path.join(base_dir, f"业务员监控及推荐官明细_{datetime.now().strftime('%Y%m%d')}.xlsx")
    try:
        recommend_df = query_to_df(SQL_RECOMMEND, "推荐官数据")
        if not recommend_df.empty:
            c, r, rec = process_data(recommend_df)
            df_to_beautiful_excel(c, r, rec, out_path)
            print(f"🎉 完成！文件已生成：{out_path}")
    except Exception: traceback.print_exc()
    input("\n按回车键退出...")

if __name__ == '__main__':
    main()
