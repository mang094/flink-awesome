package com.hw.ranking;

import com.alibaba.fastjson.JSON;

import java.sql.Date;
import java.sql.*;
import java.time.LocalDate;
import java.util.*;

public class RankingCompanySync {
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

    private Map<String, CompanyInfo> getCompanyInfo(Connection conn, List<String> companyNames) throws SQLException {
        Map<String, CompanyInfo> result = new HashMap<>();

        String placeholders = String.join(",", Collections.nCopies(companyNames.size(), "?"));
        String query = "select dlci.company_code"
                + ",dlci.company_name"
                + ",dlci.qualified_list"
                + ",dlci.iconurl"
                + ",dlci.lng_lat"
                + ",dlci.hs_ind_list"
                + ",dlci.province"
                + ",dlci.city"
                + ",dlci.area"
                + ",dmms.score as total_score"
                + " from dws.dws_lget_company_info dlci"
                + " left join ("
                + "     select * from dm_modd.dm_migrate_mod_score"
                + "     where is_delete = 0 and label = '总分'"
                + " ) dmms on dlci.company_code = dmms.company_code"
                + " where dlci.company_name in (" + placeholders + ")";

        try (PreparedStatement pstmt = conn.prepareStatement(query)) {
            for (int i = 0; i < companyNames.size(); i++) {
                pstmt.setString(i + 1, companyNames.get(i));
            }

            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                CompanyInfo info = new CompanyInfo(rs);
                result.put(info.companyName, info);
            }
        }
        System.out.println("获取公司信息完成:" + JSON.toJSONString(result));
        return result;
    }

    private int insertRankingList(Connection conn, String title, String rankType, LocalDate publishDate, Integer count,
            String publishOrganization, String industry)
            throws SQLException {
        String query = "INSERT INTO ads.ads_ranking_list ("
                + "    title, type, publish_date, company_count, publish_organization, industry"
                + ") VALUES (?, ?, ?, ?, ?, ?)"
                + " RETURNING id";

        try (PreparedStatement pstmt = conn.prepareStatement(query)) {
            pstmt.setString(1, title);
            pstmt.setString(2, rankType);
            pstmt.setDate(3, Date.valueOf(publishDate));
            pstmt.setInt(4, count);
            pstmt.setString(5, publishOrganization);
            pstmt.setString(6, industry);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                System.out.println("新增榜单完成:" + rs.getInt(1));
                return rs.getInt(1);
            }
            throw new SQLException("Failed to get ranking list id");
        }
    }

    private void insertRankingCompanies(Connection conn, int rankingId,
            Map<String, CompanyInfo> companyInfoMap, List<String> allCompanyNames) throws SQLException {
        String queryWithInfo = "INSERT INTO ads.ads_ranking_company ("
                + "    ranking_id, sort_no, company_code, company_name,"
                + "    iconurl, hs_ind_list, lng_lat, province, city, area,"
                + "    total_score, create_time, update_time,qualified_list"
                + ") VALUES (?, ?, ?, ?, ?, ?::jsonb, ?, ?, ?, ?, ?,"
                + "         CURRENT_TIMESTAMP, CURRENT_TIMESTAMP,?::jsonb)";

        String queryWithoutInfo = "INSERT INTO ads.ads_ranking_company ("
                + "    ranking_id, sort_no, company_code, company_name,"
                + "    create_time, update_time"
                + ") VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)";

        try (PreparedStatement pstmtWithInfo = conn.prepareStatement(queryWithInfo);
                PreparedStatement pstmtWithoutInfo = conn.prepareStatement(queryWithoutInfo)) {

            for (int i = 0; i < allCompanyNames.size(); i++) {
                String companyName = allCompanyNames.get(i);
                CompanyInfo info = companyInfoMap.get(companyName);

                if (info != null) {
                    pstmtWithInfo.setInt(1, rankingId);
                    pstmtWithInfo.setInt(2, i + 1);
                    pstmtWithInfo.setString(3, info.companyCode);
                    pstmtWithInfo.setString(4, info.companyName);
                    pstmtWithInfo.setString(5, info.iconUrl);
                    pstmtWithInfo.setString(6, info.hsIndList);
                    pstmtWithInfo.setString(7, info.lngLat);
                    pstmtWithInfo.setString(8, info.province);
                    pstmtWithInfo.setString(9, info.city);
                    pstmtWithInfo.setString(10, info.area);
                    pstmtWithInfo.setDouble(11, info.totalScore);
                    pstmtWithInfo.setString(12, info.qualifiedList);
                    pstmtWithInfo.executeUpdate();
                } else {
                    pstmtWithoutInfo.setInt(1, rankingId);
                    pstmtWithoutInfo.setInt(2, i + 1);
                    pstmtWithoutInfo.setString(3, "-1");
                    pstmtWithoutInfo.setString(4, companyName);
                    pstmtWithoutInfo.executeUpdate();
                }
            }
        }
    }

    public void syncRankingCompanies(List<String> companyNames, String title,
            String rankType, LocalDate publishDate, String publishOrganization, String industry) {
        try (Connection sourceConn = getSourceConnection();
                Connection targetConn = getTargetConnection()) {

            targetConn.setAutoCommit(false);
            try {
                // 查询公司信息
                Map<String, CompanyInfo> companyInfoMap = getCompanyInfo(sourceConn, companyNames);

                // 插入榜单信息
                int rankingId = insertRankingList(targetConn, title, rankType, publishDate, companyNames.size(),
                        publishOrganization, industry);

                // 插入公司榜单关系
                insertRankingCompanies(targetConn, rankingId, companyInfoMap, companyNames);

                targetConn.commit();
                System.out.println("Successfully created ranking list with ID: " + rankingId);
            } catch (Exception e) {
                targetConn.rollback();
                throw e;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        RankingCompanySync sync = new RankingCompanySync();

        // 示例使用
        List<String> companyNames = Arrays.asList(
                "广州白云山医药集团股份有限公司"
                ,"修正药业集团股份有限公司"
                ,"云南白药集团股份有限公司"
                ,"江西济民可信医药有限公司"
                ,"中国北京同仁堂(集团)有限责任公司"
                ,"华润三九医药股份有限公司"
                ,"山东步长制药股份有限公司"
                ,"重庆太极实业(集团)股份有限公司"
                ,"丽珠医药集团股份有限公司"
                ,"石家庄以岭药业股份有限公司"
                ,"中国中药控股有限公司"
                ,"扬子江药业集团江苏龙凤堂中药有限公司"
                ,"江苏济川控股集团有限公司"
                ,"漳州片仔癀药业股份有限公司"
                ,"天士力医药集团股份有限公司"
                ,"昆药集团股份有限公司"
                ,"津药达仁堂集团股份有限公司"
                ,"天津红日药业股份有限公司"
                ,"浙江康恩贝制药股份有限公司"
                ,"仁和药业股份有限公司"
                ,"葵花药业集团股份有限公司"
                ,"江苏康缘药业股份有限公司"
                ,"黑龙江珍宝岛药业股份有限公司"
                ,"东阿阿胶股份有限公司"
                ,"株洲千金药业股份有限公司"
                ,"鲁南制药集团股份有限公司"
                ,"四川好医生攀西药业有限责任公司"
                ,"上海雷允上药业有限公司"
                ,"江中药业股份有限公司"
                ,"雷允上药业集团有限公司"
                ,"健民药业集团股份有限公司"
                ,"贵州百灵企业集团制药股份有限公司"
                ,"马应龙药业集团股份有限公司"
                ,"苏中药业集团股份有限公司"
                ,"成都康弘药业集团股份有限公司"
                ,"九芝堂股份有限公司"
                ,"吉林万通集团有限公司"
                ,"河南羚锐制药股份有限公司"
                ,"吉林敖东药业集团股份有限公司"
                ,"贵州益佰制药股份有限公司"
                ,"亚宝药业集团股份有限公司"
                ,"广东众生药业股份有限公司"
                ,"金陵药业股份有限公司"
                ,"西藏诺迪康药业股份有限公司"
                ,"杭州中美华东制药有限公司"
                ,"青峰医药集团有限公司"
                ,"陕西丽彩药业有限公司"
                ,"上海和黄药业有限公司"
                ,"广州康臣药业有限公司"
                ,"广州一品红制药有限公司"
                ,"广州市香雪制药股份有限公司"
                ,"颈复康药业集团有限公司"
                ,"神威药业集团有限公司"
                ,"西藏奇正藏药股份有限公司"
                ,"河南太龙药业股份有限公司"
                ,"桂林三金药业股份有限公司"
                ,"浙江佐力药业股份有限公司"
                ,"万邦德医药控股集团股份有限公司"
                ,"上海医药集团青岛国风药业股份有限公司"
                ,"清华德人西安幸福制药有限公司"
                ,"海南葫芦娃药业集团股份有限公司"
                ,"仲景宛西制药股份有限公司"
                ,"重庆希尔安药业有限公司"
                ,"精华制药集团股份有限公司"
                ,"山东宏济堂制药集团股份有限公司"
                ,"通化金马药业集团股份有限公司"
                ,"正大青春宝药业有限公司"
                ,"湖南方盛制药股份有限公司"
                ,"河南润弘本草制药有限公司"
                ,"贵州三力制药股份有限公司"
                ,"广西梧州制药(集团)股份有限公司"
                ,"吉林华康药业股份有限公司"
                ,"上海凯宝药业股份有限公司"
                ,"贵阳新天药业股份有限公司"
                ,"北京北大维信生物科技有限公司"
                ,"北京春风药业有限公司"
                ,"兰州佛慈制药股份有限公司"
                ,"山东沃华医药科技股份有限公司"
                ,"辽宁上药好护士药业(集团)有限公司"
                ,"广誉远中药股份有限公司"
                ,"陕西盘龙药业集团股份有限公司"
                ,"湖南汉森制药股份有限公司"
                ,"杭州胡庆余堂药业有限公司"
                ,"特一药业集团股份有限公司"
                ,"成都华神科技集团股份有限公司"
                ,"吉林省集安益盛药业股份有限公司"
                ,"邯郸制药股份有限公司"
                ,"重庆华森制药股份有限公司"
                ,"江苏九旭药业有限公司"
                ,"贵州信邦制药股份有限公司"
                ,"赛灵药业科技集团股份有限公司"
                ,"恩威医药股份有限公司"
                ,"广东嘉应制药股份有限公司"
                ,"厦门中药厂有限公司"
                ,"广东红珊瑚药业有限公司"
                ,"云南生物谷药业股份有限公司"
                ,"河南福森药业有限公司"
                ,"金花企业(集团)股份有限公司"
                ,"长白山制药股份有限公司"
                ,"浙江永宁药业股份有限公司"

        );
        String title = "2023医疗器械企业创新TOP100";
        String rankType = "国家层级权威榜单"; // 或 "国家权威榜"
        LocalDate publishDate = LocalDate.of(2023, 12, 9);
        String publishOrganization = "中国中药协会";
        String industry = "生物医药";
        sync.syncRankingCompanies(companyNames, title, rankType, publishDate, publishOrganization, industry);
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


}
