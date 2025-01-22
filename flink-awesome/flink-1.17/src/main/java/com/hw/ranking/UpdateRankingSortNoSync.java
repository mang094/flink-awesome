package com.hw.ranking;

import java.sql.*;
import java.util.Arrays;
import java.util.List;

/**
 * @ClassName UpdateRankingSortNoSync
 * @Description TODO
 * @Author zh
 * @Date 2024/12/27 16:19
 * @Version
 */
public class UpdateRankingSortNoSync {
    // 源数据库配置
    private static final String SOURCE_URL = "jdbc:postgresql://192.168.201.30:1921/dm";
    private static final String SOURCE_USER = "dm_sync";
    private static final String SOURCE_PASSWORD = "DjbsHt2oY)l2V40j";

    // 目标数据库配置
    private static final String TARGET_URL = "jdbc:postgresql://192.168.200.69:5432/brain_saas";
    private static final String TARGET_USER = "brain_saas";
    private static final String TARGET_PASSWORD = "bjbsHt2oY)l2V40j";

    public static class CompanyInfo {
        String companyCode;
        String companyName;
        String iconUrl;
        String lngLat;
        String hsIndList;
        String province;
        String city;
        String area;
        Double totalScore;
        String qualifiedList;

        public CompanyInfo(ResultSet rs) throws SQLException {
            this.companyCode = rs.getString("company_code");
            this.companyName = rs.getString("company_name");
            this.iconUrl = rs.getString("iconurl");
            this.lngLat = rs.getString("lng_lat");
            this.hsIndList = rs.getString("hs_ind_list");
            this.province = rs.getString("province");
            this.city = rs.getString("city");
            this.area = rs.getString("area");
            this.totalScore = rs.getDouble("total_score");
            this.qualifiedList = rs.getString("qualified_list");
        }
    }

    private Connection getSourceConnection() throws SQLException {
        return DriverManager.getConnection(SOURCE_URL, SOURCE_USER, SOURCE_PASSWORD);
    }

    private Connection getTargetConnection() throws SQLException {
        return DriverManager.getConnection(TARGET_URL, TARGET_USER, TARGET_PASSWORD);
    }

    private void updateRankingSortNo(Connection conn, int rankingId, List<String> companyNames) throws SQLException {
        String sql = "UPDATE ads.ads_ranking_company SET sort_no = ? WHERE ranking_id = ? AND company_name = ?";

        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            for (int i = 0; i < companyNames.size(); i++) {
                pstmt.setInt(1, i + 1); // sort_no starts from 1
                pstmt.setInt(2, rankingId);
                pstmt.setString(3, companyNames.get(i));
                pstmt.addBatch();
            }
            pstmt.executeBatch();
        }
    }

    public void updateRankingOrder(int rankingId, List<String> companyNames) {
        try (Connection conn = getTargetConnection()) {
            conn.setAutoCommit(false);
            try {
                updateRankingSortNo(conn, rankingId, companyNames);
                conn.commit();
                System.out.println("Successfully updated ranking order for ranking ID: " + rankingId);
            } catch (Exception e) {
                conn.rollback();
                throw e;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        RankingCompanySync sync = new RankingCompanySync();

        // Example usage for updating ranking order
        int rankingId = 13; // Replace with actual ranking ID
        List<String> companyNames = Arrays.asList(
                "深圳惠泰医疗器械股份有限公司"
                ,"箕星药业科技(上海)有限公司"
                ,"北京蓝帆柏盛医疗科技股份有限公司"
                ,"上海昂阔医药有限公司"
                ,"江苏威凯尔医药科技股份有限公司"
                ,"上海辐联医药科技有限公司"
                ,"北京数字精准医疗科技有限公司"
                ,"南京宁丹新药技术有限公司"
                ,"安徽智医慧云科技有限公司"
                ,"上海超群检测科技股份有限公司"
                ,"上海循曜生物科技有限公司"
                ,"上海镁睿科技有限公司"
                ,"大连依利特分析仪器有限公司"
                ,"福建自贸试验区厦门片区Manteia数据科技有限公司"
                ,"陕西佰傲再生医学有限公司"
                ,"上海励楷科技有限公司"
                ,"杭州英术生命科技有限公司"
                ,"上海惠影医疗科技有限公司"
                ,"宁波琳盛高分子材料有限公司"
                ,"深圳汉诺医疗科技股份有限公司"
                ,"甫康(上海)健康科技有限责任公司"
                ,"上海柯君医药科技有限公司"
                ,"浙江达普生物科技有限公司"
                ,"天津远山医疗科技有限责任公司"
                ,"河北维达康生物科技有限公司"
                ,"益佳达医疗科技(上海)有限公司"
                ,"恒瑞源正(上海)生物科技有限公司"
                ,"江苏谦仁生物科技有限公司"
                ,"南京迈诺威医药科技有限公司"
                ,"佛山奥素博新科技有限公司"
                ,"天津星联肽生物科技有限公司"
                ,"广西加分器科技有限公司"
                ,"广东龙创基药业有限公司"
                ,"音科思(深圳)技术有限公司"
                ,"中科搏锐(北京)科技有限公司"
                ,"深圳奥礼生物科技有限公司"
                ,"北京安龙生物医药有限公司"
                ,"上海泌码生命科学有限公司"
                ,"昆山益腾医疗科技有限公司"
                ,"中山恒赛生物科技有限公司"
                ,"深圳虹信生物科技有限公司"
                ,"迈英诺医药科技(珠海)有限公司"
                ,"清超卓影(北京)医疗科技有限公司"
                ,"上海英捷信医疗科技有限公司"
                ,"深圳市星辰海医疗科技有限公司"
                ,"特科罗生物科技(成都)有限公司"
                ,"成都诺恩生物科技有限公司"
                ,"江苏富翰医疗产业发展有限公司"
                ,"诺灵生物医药科技(北京)有限公司"
                ,"苏州麦锐克生物科技有限公司"


                // Add more companies as needed
        );

        sync.updateRankingOrder(rankingId, companyNames);
    }

}

