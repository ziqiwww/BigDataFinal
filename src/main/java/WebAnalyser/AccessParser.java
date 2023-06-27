package WebAnalyser;

import java.util.HashMap;
import java.util.Map;

public class AccessParser {
    static private class LocalTime {

        public int year;
        public int month;
        public int day;
        public int hour;
        public int minute;
        public int second;
        public int timezone;

        public LocalTime(String time) {
            // example format: 18/Sep/2013:07:42:19 +0000
            Map<String, Integer> monthMap = new HashMap<String, Integer>();
            monthMap.put("Jan", 1);
            monthMap.put("Feb", 2);
            monthMap.put("Mar", 3);
            monthMap.put("Apr", 4);
            monthMap.put("May", 5);
            monthMap.put("Jun", 6);
            monthMap.put("Jul", 7);
            monthMap.put("Aug", 8);
            monthMap.put("Sep", 9);
            monthMap.put("Oct", 10);
            monthMap.put("Nov", 11);
            monthMap.put("Dec", 12);
            String[] arr = time.split(" ");
            String[] times = arr[0].split(":");
            String[] date = times[0].split("/");
            this.day = Integer.parseInt(date[0]);
            this.month = monthMap.get(date[1]);
            this.year = Integer.parseInt(date[2]);
            this.hour = Integer.parseInt(times[1]);
            this.minute = Integer.parseInt(times[2]);
            this.second = Integer.parseInt(times[3]);
            this.timezone = Integer.parseInt(arr[1]);
        }
    }

    private final String line;
    private String remote_addr;
    private String remote_user;
    private LocalTime time_local;
    private String request;
    private int status;
    private long body_bytes_sent;
    private String http_referer;
    private String http_user_agent;

    public AccessParser(String line) {
        this.line = line;
    }

    public String getRemote_addr() {
        return remote_addr;
    }

    public String getRemote_user() {
        return remote_user;
    }

    public String getTimeInHour() {
        return String.format("%04d-%02d-%02d-%02d", time_local.year, time_local.month, time_local.day, time_local.hour);
    }

    public String getRequest() {
        return request;
    }

    public String getDevice() {
        String device = "Other";
        if (http_user_agent.contains("Android")) {
            device = "Android";
        } else if (http_user_agent.contains("iPhone")) {
            device = "iPhone";
        } else if (http_user_agent.contains("iPad")) {
            device = "iPad";
        } else if (http_user_agent.contains("Windows Phone")) {
            device = "Windows Phone";
        } else if (http_user_agent.contains("Windows NT")) {
            device = "Windows";
        } else if (http_user_agent.contains("Macintosh")) {
            device = "Macintosh";
        } else if (http_user_agent.contains("Linux")) {
            device = "Linux";
        } else if (http_user_agent.contains("spider")) {
            device = "spider";
        } else if (http_user_agent.contains("compatible")) {
            device = "compatible";
        }
        return device;
    }

    public int getStatus() {
        return status;
    }

    public long getBody_bytes_sent() {
        return body_bytes_sent;
    }

    public String getHttp_referer() {
        return http_referer;
    }

    public String getHttp_user_agent() {
        return http_user_agent;
    }

    public boolean parse() {
        // example format: 125.35.62.66 - - [18/Sep/2013:07:42:19 +0000] "GET /wp-content/uploads/2013/06/upstart.png HTTP/1.1" 200 22204 "http://blog.chinaunix.net/uid-8746761-id-3846816.html" "Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; Trident/6.0; EIE10;ENUSWOL)"
        String[] arrSpace = line.split(" ");
        String[] arrQuote = line.split("\"");
        if (arrSpace.length < 11 || arrQuote.length < 6) {
            return false;
        }
        remote_addr = arrSpace[0];
        remote_user = arrSpace[1];
        String timeStr = arrSpace[3].substring(1) + " " + arrSpace[4].substring(0, arrSpace[4].length() - 1);
        time_local = new LocalTime(timeStr);
        request = arrQuote[1];
        String statusBytesStr = arrQuote[2].substring(1, arrQuote[2].length() - 1);
        String[] arrStatusBytes = statusBytesStr.split(" ");
        status = Integer.parseInt(arrStatusBytes[0]);
        body_bytes_sent = Long.parseLong(arrStatusBytes[1]);
        http_referer = arrQuote[3];
        http_user_agent = arrQuote[5];
        return true;
    }
}
