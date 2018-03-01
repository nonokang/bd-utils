package org.bd.solr;

import java.beans.PropertyDescriptor;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.pagehelper.Page;

/**
 * <b>版权信息:</b> 广州智数信息科技有限公司<br>
 * <b>功能描述:</b> 全文搜索帮助类，查询（查询语句构造），更新，重建索引<br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年12月25日 上午10:48:01 |创建
 */
public class SolrHelper<T> {

    protected final Logger logger = LoggerFactory.getLogger(SolrHelper.class);
 
    private HttpSolrClient server;
 
    private StringBuffer queryString;
 
    public SolrHelper(String reqUrl) {
        server = new HttpSolrClient(reqUrl);
        queryString = new StringBuffer();
    }
 
    public void andEquals(String fieldName, String val) {
        queryString.append(" && ").append(fieldName).append(":").append(val);
    }
 
    public void orEquals(String fieldName, String val) {
        queryString.append(" || ").append(fieldName).append(":").append(val);
    }
 
    public void andNotEquals(String fieldName, String val) {
        queryString.append(" && ").append("-").append(fieldName).append(":").append(val);
    }
 
    public void orNotEquals(String fieldName, String val) {
        queryString.append(" || ").append("-").append(fieldName).append(":").append(val);
    }
 
    public void andGreaterThan(String fieldName, String val) {
        queryString.append(" && ").append(fieldName).append(":[").append(val).append(" TO ").append("*]");
    }
 
    public void orGreaterThan(String fieldName, String val) {
        queryString.append(" || ").append(fieldName).append(":[").append(val).append(" TO ").append("*]");
    }
 
    public void andGreaterThanOrEqualTo(String fieldName, String val) {
        queryString.append(" && ").append(fieldName).append(":[").append(val).append(" TO ").append("*]");
    }
 
    public void orGreaterThanOrEqualTo(String fieldName, String val) {
        queryString.append(" || ").append(fieldName).append(":[").append(val).append(" TO ").append("*]");
    }
 
    public void andDateGreaterThan(String fieldName, Date val) {
        queryString.append(" && ").append(fieldName).append(":[").append(formatUTCString(val)).append(" TO ").append("*]");
    }
 
    public void orDateGreaterThan(String fieldName, Date val) {
        queryString.append(" || ").append(fieldName).append(":[").append(formatUTCString(val)).append(" TO ").append("*]");
    }
 
    public void andDateGreaterThanOrEqualTo(String fieldName, Date val) {
        queryString.append(" && ").append(fieldName).append(":[").append(formatUTCString(val)).append(" TO ").append("*]");
    }
 
    public void orDateGreaterThanOrEqualTo(String fieldName, Date val) {
        queryString.append(" || ").append(fieldName).append(":[").append(formatUTCString(val)).append(" TO ").append("*]");
    }
 
    public void andLessThan(String fieldName, String val) {
        queryString.append(" && ").append(fieldName).append(":[").append("*").append(" TO ").append(val).append("]");
    }
 
    public void orLessThan(String fieldName, String val) {
        queryString.append(" && ").append(fieldName).append(":[").append("*").append(" TO ").append(val).append("]");
    }
 
    public void andLessThanOrEqualTo(String fieldName, String val) {
        queryString.append(" && ").append(fieldName).append(":[").append("*").append(" TO ").append(val).append("]");
    }
 
    public void orLessThanOrEqualTo(String fieldName, String val) {
        queryString.append(" && ").append(fieldName).append(":[").append("*").append(" TO ").append(val).append("]");
    }
 
    public void andDateLessThan(String fieldName, Date val) {
        queryString.append(" && ").append(fieldName).append(":[").append("*").append(" TO ").append(formatUTCString(val)).append("]");
    }
 
    public void orDateLessThan(String fieldName, Date val) {
        queryString.append(" && ").append(fieldName).append(":[").append("*").append(" TO ").append(formatUTCString(val)).append("]");
    }
 
    public void andDateLessThanOrEqualTo(String fieldName, Date val) {
        queryString.append(" && ").append(fieldName).append(":[").append("*").append(" TO ").append(formatUTCString(val)).append("]");
    }
 
    public void orDateLessThanOrEqualTo(String fieldName, Date val) {
        queryString.append(" && ").append(fieldName).append(":[").append("*").append(" TO ").append(formatUTCString(val)).append("]");
    }
 
    public void andLike(String fieldName, String val) {
        queryString.append(" && ").append(fieldName).append(":*").append(val).append("*");
    }
 
    public void orLike(String fieldName, String val) {
        queryString.append(" || ").append(fieldName).append(":*").append(val).append("*");
    }
 
    public void andNotLike(String fieldName, String val) {
        queryString.append(" && ").append("-").append(fieldName).append(":*").append(val).append("*");
    }
 
    public void orNotLike(String fieldName, String val) {
        queryString.append(" || ").append("-").append(fieldName).append(":*").append(val).append("*");
    }
 
    public void andIn(String fieldName, String[] vals) {
        queryString.append(" && ");
        in(fieldName, vals);
    }
    
    private void in(String fieldName, String[] vals) {
        List<String> list=Arrays.asList(vals);
        in(queryString,fieldName,list);
    }
     
    public void orIn(String fieldName, List<String> vals) {
        queryString.append(" || ");
        in(queryString,fieldName,vals);
    }
 
 
    private static void in(StringBuffer queryString,String fieldName, List<String> vals) {
        queryString.append("(");
        inStr(queryString, fieldName, vals);
        queryString.append(")");
    }
 
    private static void inStr(StringBuffer queryString, String fieldName, List<String> vals) {
        int index = 0;
        for (String val : vals) {
            if (0 != index) {
                queryString.append(" || ");
            }
            queryString.append(fieldName).append(":").append(val);
            index++;
        }
    }
     
    // https://stackoverflow.com/questions/634765/using-or-and-not-in-solr-query
    //instead of NOT [condition] use (*:* NOT [condition])
    public void andNotIn(String fieldName, String[] vals) {
        List<String> list=Arrays.asList(vals);
        queryString.append("&&(");
        queryString.append("*:* NOT ");
        inStr(queryString, fieldName, list);
        queryString.append(")");
    }
 
    public void andDateBetween(String fieldName, Date startDate, Date endDate) {
        queryString.append(" && ").append(fieldName).append(":[")
                .append(formatUTCString(startDate)).append(" TO ")
                .append(formatUTCString(endDate)).append("]");
    }
 
    public void orDateBetween(String fieldName, Date startDate, Date endDate) {
        queryString.append(" || ").append(fieldName).append(":[")
                .append(formatUTCString(startDate)).append(" TO ")
                .append(formatUTCString(endDate)).append("]");
    }
 
    public void andDateNotBetween(String fieldName, Date startDate, Date endDate) {
        queryString.append(" && ").append("-").append(fieldName).append(":[")
                .append(formatUTCString(startDate)).append(" TO ")
                .append(formatUTCString(endDate)).append("]");
    }
 
    public void orDateNotBetween(String fieldName, Date startDate, Date endDate) {
        queryString.append(" && ").append("-").append(fieldName).append(":[")
                .append(formatUTCString(startDate)).append(" TO ")
                .append(formatUTCString(endDate)).append("]");
    }
 
    public void andBetween(String fieldName, String start, String end) {
        queryString.append(" && ").append(fieldName).append(":[").append(start)
                .append(" TO ").append(end).append("]");
    }
 
    public void orBetween(String fieldName, String start, String end) {
        queryString.append(" || ").append(fieldName).append(":[").append(start)
                .append(" TO ").append(end).append("]");
    }
 
    public void andNotBetween(String fieldName, String start, String end) {
        queryString.append(" && ").append("-").append(fieldName).append(":[")
                .append(start).append(" TO ").append(end).append("]");
    }
 
    public void orNotBetween(String fieldName, String start, String end) {
        queryString.append(" || ").append("-").append(fieldName).append(":[")
                .append(start).append(" TO ").append(end).append("]");
    }
 
    public void andStartSub() {
        queryString.append(" && (");
    }
 
    public void orStartSub() {
        queryString.append(" || (");
    }
 
    public void endSub() {
        queryString.append(")");
    }
 
    private String formatUTCString(Date d) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        String s = sdf.format(d);
        return s;
    }
 
    public int execQueryTotalCount() {
        SolrQuery params = handleQuery();
        params.set("start", 0);
        params.set("rows", Integer.MAX_VALUE);
        QueryResponse response = null;
        try {
            response = server.query(params);
            return response.getResults().size();
        } catch (SolrServerException e) {
            logger.error("", e);
        } catch (IOException e) {
            logger.error("", e);
        }
        return 0;
    }
 
    public List<T> query(String sort, Class<T> beanClass) {
        SolrQuery params = handleQuery();
        QueryResponse response = null;
        List<T> list = null;
        try {
            logger.info("SolyQuery:" + params.toString());
            response = server.query(params);
            list = (List<T>) response.getBeans(beanClass);
        } catch (SolrServerException e) {
            logger.error("SolrServerException", e);
        } catch (IOException e) {
            logger.error("IOException", e);
        }
        return list;
    }
 
    public Page<T> execQuery(Integer pageNo, Integer rows, String sort, Class<T> beanClass) {
        List<T> results = null;
        Page<T> page = null;
        SolrQuery params = handleQuery();
        if (pageNo != null && rows != null && pageNo > 0 && rows > 0) {
            params.set("start", (pageNo - 1) * rows);
            params.set("rows", rows);
        }
        if (null != sort && !"".equals(sort)) {
            params.set(sort, sort);
        }
 
        QueryResponse response = null;
        try {
            logger.info("SolyQuery WithPage:" + params.toString());
            response = server.query(params);
            results = (List<T>) response.getBeans(beanClass);
            page = new Page<T>(pageNo, rows);
//            page = new Page<T>(pageNo, rows, execQueryTotalCount());
            page.addAll(results);
        } catch (SolrServerException e) {
            logger.error("SolrServerException", e);
        } catch (IOException e) {
            logger.error("IOException", e);
        }
 
        return page;
    }
 
    private SolrQuery handleQuery() {
        SolrQuery params = new SolrQuery();
        String qryFinalStr = queryString.toString();
        if (qryFinalStr.startsWith(" && ")) {
            qryFinalStr = qryFinalStr.replaceFirst(" && ", "");
        } else if (qryFinalStr.startsWith(" || ")) {
            qryFinalStr = qryFinalStr.replaceFirst(" || ", "");
        }
        // 子查询开头的关联符号
        if (-1 != qryFinalStr.indexOf((" && "))) {
            qryFinalStr = qryFinalStr.replaceAll("\\( \\&\\& ", "(");
        }
        if (-1 != qryFinalStr.indexOf((" || "))) {
            qryFinalStr = qryFinalStr.replaceAll("\\( \\|\\| ", "(");
        }
        if (StringUtils.isBlank(qryFinalStr)) {
            qryFinalStr = "*:*";
        }
        params.set("q", qryFinalStr);
        return params;
    }
 
    public void execDelete(String keyName, String keyVal) {
        try {
            server.deleteByQuery(keyName + ":" + keyVal);
            server.commit();
        } catch (SolrServerException | IOException e) {
            logger.error("", e);
        }
    }
 
    public void execUpdate(T model) {
        Field[] fields = model.getClass().getDeclaredFields();
        SolrInputDocument solrDoc = new SolrInputDocument();
        try {
            for (Field f : fields) {
                PropertyDescriptor pd;
                pd = new PropertyDescriptor(f.getName(), model.getClass());
                // 属性名
                String fieldName = f.getName();
                Method rM = pd.getReadMethod();// 获得读方法
                solrDoc.addField(fieldName, rM.invoke(model));
            }
            server.add(solrDoc);
            server.commit();
        } catch (Exception e) {
            logger.error("", e);
        }
    }
 
    public void execUpdate(SolrInputDocument solrDoc) {
        try {
            server.add(solrDoc);
            server.commit();
        } catch (SolrServerException e) {
            logger.error("", e);
        } catch (IOException e) {
            logger.error("", e);
        }
    }
 
    /**
     * 重建索引和增量索引的接口
     * @param delta
     */
    public void buildIndex(boolean delta) {
        SolrQuery query = new SolrQuery();
        // 指定RequestHandler，默认使用/select
        query.setRequestHandler("/dataimport");
 
        String command = delta ? "delta-import" : "full-import";
        String clean = delta ? "false" : "true";
        String optimize = delta ? "false" : "true";
 
        query.setParam("command", command).setParam("clean", clean)
                .setParam("commit", true).setParam("optimize", optimize);
        try {
            server.query(query);
        } catch (SolrServerException e) {
            logger.error("建立索引时遇到错误，delta:" + delta, e);
        } catch (IOException e) {
            logger.error("建立索引时遇到错误，delta:" + delta, e);
        }
    }
}
