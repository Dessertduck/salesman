# -*- coding: utf-8 -*-
"""
业务员监控报表自动生成脚本
双击 exe 即可自动从数据库拉取数据并生成 Excel 报表
"""

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
    except pymysql.Error as e:
        print(f"❌ {df_name} 数据库操作失败：{e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"❌ {df_name} 程序执行失败：{e}")
        return pd.DataFrame()


# ────────────────────────────────────────────────
# 3. SQL 语句
# ────────────────────────────────────────────────
SQL_RECOMMEND = """
WITH base AS (
  SELECT
    r.order_num                              AS 订单编号,
    r.openid                                 AS 推荐官openid,
    r.invitee_openid                         AS 被推荐人openid,
    yu.nick_name                             AS 名字,
    yu.phone_num                             AS 电话,
    recommender_u.nick_name                  AS 推荐官姓名,
    recommender_u.phone_num                  AS 推荐官手机号,
    rc.city_name                             AS 推荐官城市,
    r.biz_manager                            AS 业务员openid,
    biz_u.nick_name                          AS 业务员姓名,
    CASE
      WHEN c.city_name = '台州市' THEN '温州市'
      WHEN c.city_name = '鄂州市'  THEN '武汉市'
      ELSE COALESCE(c.city_name, '')
    END                                      AS 费用归属城市,
    ROUND(r.order_amount / 100, 2)           AS 订单金额,
    ROUND(r.reward / 100, 2)                 AS 收益,
    r.create_time                            AS 下单时间,
    r.order_type                             AS 原始订单类型,
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
      WHEN r.record_type = 'master'         THEN '主记录'
      WHEN r.record_type = 'unbind_rentals' THEN '退租'
      WHEN r.record_type = 'adjustment'     THEN '调整单'
      WHEN r.record_type = 'forfeiture'     THEN '作废'
      ELSE r.record_type
    END                                      AS 记录类型,
    r.status                                 AS 状态,
    COALESCE(r.adjust_reasons, '无')         AS 调整说明
  FROM yzl_recommender_reward r
  LEFT JOIN yzl_city_setting c    ON r.reward_city_id = c.id
  LEFT JOIN yzl_user yu           ON r.invitee_openid = yu.openid
  LEFT JOIN yzl_user recommender_u ON r.openid = recommender_u.openid
  LEFT JOIN yzl_recommender yr    ON r.openid = yr.openid
  LEFT JOIN yzl_city_setting rc   ON yr.city_id = rc.id
  LEFT JOIN yzl_user biz_u        ON r.biz_manager = biz_u.openid
  WHERE r.is_deleted = 0
    AND r.status != 'deleted'
),

non_special_flagged AS (
  SELECT
    b.*,
    CASE
      WHEN COUNT(*) OVER (PARTITION BY b.订单编号) = 1 THEN 0
      WHEN COUNT(*) OVER (PARTITION BY b.订单编号) > 1
           AND b.记录类型 = '调整单'
           AND NOT (
             b.调整说明 LIKE '%退租%' OR
             b.调整说明 LIKE '%租金退款%' OR
             b.调整说明 LIKE '%老用户%'
           )
      THEN 0
      ELSE 1
    END AS 是否异常退租
  FROM base b
  WHERE b.订单编号 NOT IN ('1988254627647758336','BRO1991160250767929344')
),

special_flagged AS (
  SELECT
    t.订单编号, t.推荐官openid, t.被推荐人openid, t.名字, t.电话,
    t.推荐官姓名, t.推荐官手机号, t.推荐官城市, t.业务员openid, t.业务员姓名,
    t.费用归属城市, t.订单金额, t.收益, t.下单时间, t.原始订单类型, t.计费类型,
    t.渠道等级, t.记录类型, t.状态, t.调整说明,
    0 AS 是否异常退租
  FROM (
    SELECT
      b.*,
      ROW_NUMBER() OVER (PARTITION BY b.订单编号 ORDER BY b.下单时间 DESC) AS rn
    FROM base b
    WHERE b.订单编号 IN ('1988254627647758336','BRO1991160250767929344')
  ) t
  WHERE t.rn = 1
),

final_detail AS (
  SELECT 订单编号, 推荐官openid, 被推荐人openid, 名字, 电话,
         推荐官姓名, 推荐官手机号, 推荐官城市, 业务员openid, 业务员姓名,
         费用归属城市, 订单金额, 收益, 下单时间, 原始订单类型, 计费类型,
         渠道等级, 记录类型, 状态, 调整说明, 是否异常退租
  FROM non_special_flagged

  UNION ALL

  SELECT 订单编号, 推荐官openid, 被推荐人openid, 名字, 电话,
         推荐官姓名, 推荐官手机号, 推荐官城市, 业务员openid, 业务员姓名,
         费用归属城市, 订单金额, 收益, 下单时间, 原始订单类型, 计费类型,
         渠道等级, 记录类型, 状态, 调整说明, 是否异常退租
  FROM special_flagged
)

SELECT
  订单编号, 推荐官openid, 被推荐人openid, 名字, 电话,
  推荐官姓名, 推荐官手机号, 推荐官城市, 业务员openid, 业务员姓名,
  费用归属城市, 订单金额, 收益, 下单时间, 计费类型, 渠道等级, 记录类型, 状态
FROM final_detail
WHERE 是否异常退租 = 0
ORDER BY 下单时间;
"""


# ────────────────────────────────────────────────
# 4. 数据处理
# ────────────────────────────────────────────────
def process_data(recommend_df: pd.DataFrame):
    # 时间窗口
    today = pd.Timestamp.now().normalize()
    curr_month_start = today.replace(day=1)
    yesterday_start  = today - pd.Timedelta(days=1)
    yesterday_end    = today

    # 剔除指定城市
    df = recommend_df.copy()
    df = df[~df['推荐官城市'].isin(['合肥市', '金华市'])]
    df['下单时间'] = pd.to_datetime(df['下单时间'])

    # 统计函数（城市汇总用）
    def calc_stats(data, start, end):
        mask = (data['下单时间'] >= start) & (data['下单时间'] < end)
        period = data[mask].copy()
        if period.empty:
            return pd.DataFrame(columns=['推荐官人数', '拉新人数', '活跃推荐官人数'])
        valid = period[period['计费类型'] == '有效拉新']
        total   = period.groupby('业务员姓名')['推荐官openid'].nunique()
        pulls   = valid.groupby('业务员姓名')['被推荐人openid'].nunique()
        inv     = valid.groupby(['业务员姓名', '推荐官openid'])['被推荐人openid'].nunique().reset_index()
        active  = inv[inv['被推荐人openid'] > 3].groupby('业务员姓名')['推荐官openid'].count()
        stats   = pd.concat([total, pulls, active], axis=1).fillna(0).astype(int)
        stats.columns = ['推荐官人数', '拉新人数', '活跃推荐官人数']
        return stats

    stats_yd  = calc_stats(df, yesterday_start, yesterday_end).add_prefix('昨日')
    stats_mtd = calc_stats(df, curr_month_start, today).add_prefix('本月至今')

    city_map = df.groupby('业务员姓名')['推荐官城市'].agg(
        lambda x: x.mode().iloc[0] if not x.mode().empty else '未知'
    ).reset_index()
    city_map.columns = ['业务员姓名', '城市']

    salesman_final = city_map.merge(stats_yd, left_on='业务员姓名', right_index=True, how='left')
    salesman_final = salesman_final.merge(stats_mtd, left_on='业务员姓名', right_index=True, how='left').fillna(0)
    int_cols = [c for c in salesman_final.columns if c not in ['业务员姓名', '城市']]
    salesman_final[int_cols] = salesman_final[int_cols].astype(int)

    # 城市汇总
    city_summary = salesman_final.groupby('城市').agg({
        '业务员姓名':           'nunique',
        '昨日推荐官人数':       'sum',
        '昨日拉新人数':         'sum',
        '昨日活跃推荐官人数':   'sum',
        '本月至今推荐官人数':   'sum',
        '本月至今拉新人数':     'sum',
        '本月至今活跃推荐官人数': 'sum',
    }).reset_index()
    city_summary.columns = [
        '城市', '业务员人数',
        '昨日推荐官人数', '昨日拉新人数', '昨日活跃推荐官人数',
        '本月至今推荐官人数', '本月至今拉新人数', '本月至今活跃推荐官人数',
    ]
    city_summary['月净增活跃推荐官人数'] = (
        city_summary['本月至今活跃推荐官人数'] - city_summary['昨日活跃推荐官人数']
    )
    final_cols = [
        '城市', '业务员人数',
        '昨日推荐官人数', '昨日活跃推荐官人数',
        '本月至今推荐官人数', '本月至今活跃推荐官人数',
        '月净增活跃推荐官人数',
        '昨日拉新人数', '本月至今拉新人数',
    ]
    city_summary = city_summary[final_cols].sort_values('本月至今拉新人数', ascending=False)

    national_row = pd.DataFrame([{
        '城市':              '全国',
        '业务员人数':        salesman_final['业务员姓名'].nunique(),
        '昨日推荐官人数':    city_summary['昨日推荐官人数'].sum(),
        '昨日活跃推荐官人数': city_summary['昨日活跃推荐官人数'].sum(),
        '本月至今推荐官人数': city_summary['本月至今推荐官人数'].sum(),
        '本月至今活跃推荐官人数': city_summary['本月至今活跃推荐官人数'].sum(),
        '月净增活跃推荐官人数': city_summary['月净增活跃推荐官人数'].sum(),
        '昨日拉新人数':      city_summary['昨日拉新人数'].sum(),
        '本月至今拉新人数':  city_summary['本月至今拉新人数'].sum(),
    }])
    final_city_report = pd.concat([city_summary, national_row], ignore_index=True)

    # 业务员明细（不含离职/待确定）
    def get_detail_stats(data, start, end):
        mask = (data['下单时间'] >= start) & (data['下单时间'] < end)
        period = data[mask].copy()
        if period.empty:
            return pd.DataFrame(columns=['推荐官人数', '拉新人数', '活跃推荐官人数'])
        valid = period[period['计费类型'] == '有效拉新']
        total  = period.groupby('业务员姓名')['推荐官openid'].nunique()
        pulls  = valid.groupby('业务员姓名')['被推荐人openid'].nunique()
        inv    = valid.groupby(['业务员姓名', '推荐官openid'])['被推荐人openid'].nunique().reset_index()
        active = inv[inv['被推荐人openid'] > 3].groupby('业务员姓名')['推荐官openid'].count()
        stats  = pd.concat([total, pulls, active], axis=1).fillna(0).astype(int)
        stats.columns = ['推荐官人数', '拉新人数', '活跃推荐官人数']
        return stats

    yd2  = get_detail_stats(df, yesterday_start, yesterday_end).add_prefix('昨日')
    mtd2 = get_detail_stats(df, curr_month_start, today).add_prefix('本月至今')

    salesman_detail = city_map.merge(yd2,  left_on='业务员姓名', right_index=True, how='left')
    salesman_detail = salesman_detail.merge(mtd2, left_on='业务员姓名', right_index=True, how='left').fillna(0)
    int_cols2 = [c for c in salesman_detail.columns if c not in ['业务员姓名', '城市']]
    salesman_detail[int_cols2] = salesman_detail[int_cols2].astype(int)
    salesman_detail['月净增活跃推荐官人数'] = (
        salesman_detail['本月至今活跃推荐官人数'] - salesman_detail['昨日活跃推荐官人数']
    )

    detail_cols = [
        '城市', '业务员姓名',
        '昨日推荐官人数', '昨日活跃推荐官人数',
        '本月至今推荐官人数', '本月至今活跃推荐官人数',
        '月净增活跃推荐官人数', '昨日拉新人数', '本月至今拉新人数',
    ]
    result_sorted = salesman_detail[detail_cols].sort_values('城市').reset_index(drop=True)

    TARGET_SALESMEN = [
        '陈浩', '方宇帆', '罗俊', '周金', '何五霞', '鲁冰煜',
        '李前同', '杨成文', '朱泓', '郭娟', '胡鹏', '刘海', '罗春梅', '沙雪梅',
        '吴磊', '臧倩', '张宏珠', '张徐月', '姬凯', '田凯特', '王超', '张晓东',
        '岑赞锁', '陈斌', '高林', '王文波', '朱寅君', '吝世井', '南通曹国庆',
        '陈海欣', '马裕纯', '叶志洋', '尹志鹏', '岳越-南京', '张小龙', '周文龙',
        '15387063275', '黄玉松', '简永富', '李正茂', '王学成', '张中梁', '姜少锋',
        '吴国翠', '张宸',
    ]
    result_final = result_sorted[result_sorted['业务员姓名'].isin(TARGET_SALESMEN)]

    return final_city_report, result_final


# ────────────────────────────────────────────────
# 5. 美化输出 Excel
# ────────────────────────────────────────────────
def df_to_beautiful_excel(city_summary, result_final, out_path='业务员监控.xlsx'):
    def wrap_header(text, max_chars=12):
        s = str(text)
        if len(s) <= max_chars:
            return s
        parts = s.split()
        if len(parts) > 1:
            half = len(parts) // 2
            return ' '.join(parts[:half]) + '\n' + ' '.join(parts[half:])
        mid = len(s) // 2
        return s[:mid] + '\n' + s[mid:]

    def display_len(wrapped):
        return max(len(line) for line in str(wrapped).split('\n'))

    with pd.ExcelWriter(out_path, engine='xlsxwriter') as writer:
        wb  = writer.book
        sht = wb.add_worksheet('Sheet1')
        writer.sheets['Sheet1'] = sht

        FONT    = 'Microsoft YaHei'
        FSIZE   = 11

        title_fmt = wb.add_format({'bold': True, 'font_name': FONT, 'font_size': 16,
                                   'align': 'left', 'valign': 'vcenter'})
        hdr_fmt   = wb.add_format({'bold': True, 'bg_color': '#4F81BD', 'font_color': 'white',
                                   'font_name': FONT, 'font_size': 11, 'align': 'center',
                                   'valign': 'vcenter', 'top': 1, 'bottom': 1, 'text_wrap': True})
        cell_fmt  = wb.add_format({'font_name': FONT, 'font_size': FSIZE, 'valign': 'vcenter',
                                   'align': 'center', 'top': 1, 'bottom': 1})
        int_fmt   = wb.add_format({'font_name': FONT, 'font_size': FSIZE, 'num_format': '0',
                                   'valign': 'vcenter', 'align': 'center', 'top': 1, 'bottom': 1})
        float_fmt = wb.add_format({'font_name': FONT, 'font_size': FSIZE, 'num_format': '0.0',
                                   'valign': 'vcenter', 'align': 'center', 'top': 1, 'bottom': 1})
        zebra_fmt = wb.add_format({'bg_color': '#F2F2F2', 'font_name': FONT, 'font_size': FSIZE,
                                   'valign': 'vcenter', 'align': 'center', 'top': 1, 'bottom': 1})

        def make_zebra(base_fmt, num_format=None):
            props = {'bg_color': '#F2F2F2', 'font_name': FONT, 'font_size': FSIZE,
                     'valign': 'vcenter', 'align': 'center', 'top': 1, 'bottom': 1}
            if num_format:
                props['num_format'] = num_format
            return wb.add_format(props)

        def write_table(df, title_text, start_row):
            # 大标题
            sht.set_row(start_row, 28)
            sht.merge_range(start_row, 0, start_row, len(df.columns) - 1, title_text, title_fmt)
            # 表头行
            hdr_row = start_row + 1
            sht.set_row(hdr_row, 36)
            wrapped = [wrap_header(c) for c in df.columns]
            for ci, w in enumerate(wrapped):
                sht.write(hdr_row, ci, w, hdr_fmt)
            # 写数据（header=False 先写进 excel，再覆写表头和样式）
            df.to_excel(writer, sheet_name='Sheet1', startrow=hdr_row + 1, startcol=0,
                         index=False, header=False)
            for ri in range(len(df)):
                row_idx = hdr_row + 1 + ri
                sht.set_row(row_idx, 20)
                zebra = (ri % 2 == 0)
                for ci, col in enumerate(df.columns):
                    val = df.iloc[ri, ci]
                    if pd.api.types.is_integer_dtype(df[col]):
                        fmt = make_zebra(int_fmt, '0') if zebra else int_fmt
                    elif pd.api.types.is_float_dtype(df[col]):
                        fmt = make_zebra(float_fmt, '0.0') if zebra else float_fmt
                    else:
                        fmt = zebra_fmt if zebra else cell_fmt
                    sht.write(row_idx, ci, val, fmt)
            return hdr_row  # 返回表头行用于 autofilter / freeze

        # 表一
        t1_hdr = write_table(city_summary, '城市业务员详情', 0)
        sht.freeze_panes(t1_hdr + 1, 0)
        sht.autofilter(t1_hdr, 0, t1_hdr + len(city_summary), len(city_summary.columns) - 1)

        # 表二（间隔两行）
        t2_start = t1_hdr + 1 + len(city_summary) + 2
        t2_hdr   = write_table(result_final, '业务员绑定推荐官明细', t2_start)
        sht.autofilter(t2_hdr, 0, t2_hdr + len(result_final), len(result_final.columns) - 1)

        # 列宽自适应
        max_cols = max(len(city_summary.columns), len(result_final.columns))
        wh1 = [wrap_header(c) for c in city_summary.columns]
        wh2 = [wrap_header(c) for c in result_final.columns]
        for i in range(max_cols):
            lens = []
            if i < len(result_final.columns):
                lens.append(result_final.iloc[:, i].astype(str).map(len).max())
                lens.append(display_len(wh2[i]))
            if i < len(city_summary.columns):
                lens.append(city_summary.iloc[:, i].astype(str).map(len).max())
                lens.append(display_len(wh1[i]))
            wide = max(8, min(int(max(lens)) + 3, 20)) if lens else 8
            sht.set_column(i, i, wide)

    print(f"✅ 报表已保存：{out_path}")


# ────────────────────────────────────────────────
# 6. 主流程
# ────────────────────────────────────────────────
def main():
    print("=" * 50)
    print("  业务员监控报表生成工具")
    print(f"  运行时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 50)

    # 确定输出路径（exe 同目录或脚本同目录）
    if getattr(sys, 'frozen', False):
        base_dir = os.path.dirname(sys.executable)
    else:
        base_dir = os.path.dirname(os.path.abspath(__file__))

    out_path = os.path.join(base_dir, f"业务员监控_{datetime.now().strftime('%Y%m%d')}.xlsx")

    try:
        print("\n[1/3] 正在从数据库拉取数据...")
        recommend_df = query_to_df(SQL_RECOMMEND, "recommend_df")
        if recommend_df.empty:
            print("❌ 数据为空，请检查数据库连接或 SQL。")
            input("\n按回车键退出...")
            return

        print("\n[2/3] 正在处理数据...")
        final_city_report, result_final = process_data(recommend_df)
        print(f"  城市汇总：{len(final_city_report)} 行")
        print(f"  业务员明细：{len(result_final)} 行")

        print(f"\n[3/3] 正在生成 Excel 报表...")
        df_to_beautiful_excel(final_city_report, result_final, out_path)

        print(f"\n🎉 完成！文件保存在：\n   {out_path}")

    except Exception:
        print("\n❌ 发生未知错误：")
        traceback.print_exc()

    input("\n按回车键退出...")


if __name__ == '__main__':
    main()
