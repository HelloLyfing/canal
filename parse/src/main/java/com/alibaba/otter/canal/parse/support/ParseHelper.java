package com.alibaba.otter.canal.parse.support;

import com.alibaba.otter.canal.parse.inbound.mysql.rds.data.BinlogFile;
import org.apache.commons.lang.StringUtils;
import org.slf4j.MDC;

import java.util.List;

/**
 * 把parse模块中通用的部分放在这里，不放到其他module模块的原因是：尽量减少对其他模块的联动依赖，否则需要替换parse模块时，因为联动依赖导致要替换的模块变多了
 * @author luyu
 * @date 2024/10/21
 */
public final class ParseHelper {

    public static String parseBinLogName(String downloadLink) {
        int startIdx = downloadLink.indexOf("/mysql-bin.");
        int endIdx = downloadLink.indexOf("?", startIdx + 1);
        if (startIdx >= 0 && endIdx > startIdx) {
            return downloadLink.substring(startIdx + 1, endIdx);
        }
        return null;
    }

    public static void formatBinlogFiles(List<BinlogFile> binlogFiles) {
        for (BinlogFile binlog : binlogFiles) {
            // example: mysql-bin.002867
            String fileName = parseBinLogName(binlog.getDownloadLink());
            binlog.setFileName(fileName);
        }
    }

    public static String getBinLogFileInfo(BinlogFile binlogFile) {
        return String.format("fileName:%s,logBegin:%s", binlogFile.getFileName(), binlogFile.getLogBeginTime());
    }

    public static void prepareLogMDC(String destination) {
        MDC.put("destination", destination);
    }

    public static void cleanLogMDC() {
        MDC.remove("destination");
    }

    public static String maskStr(String srcTxt) {
        if (StringUtils.isBlank(srcTxt)) {
            return srcTxt;
        }

        // 掩码1/4长度的内容
        int maskLen = srcTxt.length() / 4;
        int startIdx = srcTxt.length() / 2 - (maskLen / 2);
        return srcTxt.substring(0, startIdx) + "***" + srcTxt.substring(startIdx + maskLen);
    }

    public static <T> T getFirstItem(List<T> list) {
        if (list == null || list.isEmpty()) {
            return null;
        }
        return list.get(0);
    }

    public static <T> T getLastItem(List<T> list) {
        if (list == null || list.isEmpty()) {
            return null;
        }
        return list.get(list.size() - 1);
    }

}
