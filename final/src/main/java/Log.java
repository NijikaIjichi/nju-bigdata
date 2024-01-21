import io.krakens.grok.api.Grok;
import io.krakens.grok.api.GrokCompiler;
import io.krakens.grok.api.Match;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

public class Log {
  private final String request;
  private final String agent;
  private final String auth;
  private final String ident;
  private final String verb;
  private final String referrer;
  private final int response;
  private final int bytes;
  private final String clientIP;
  private final String httpVersion;
  private final String rawRequest;
  private final Date timestamp;

  private Log(String request, String agent, String auth, String ident,
              String verb, String referrer, int response, int bytes,
              String clientIP, String httpVersion, String rawRequest, Date timestamp) {
    this.request = request;
    this.agent = agent;
    this.auth = auth;
    this.ident = ident;
    this.verb = verb;
    this.referrer = referrer;
    this.response = response;
    this.bytes = bytes;
    this.clientIP = clientIP;
    this.httpVersion = httpVersion;
    this.rawRequest = rawRequest;
    this.timestamp = timestamp;
  }

  static public Log parseLog(String log) throws ParseException {
    GrokCompiler grokCompiler = GrokCompiler.newInstance();
    grokCompiler.registerDefaultPatterns();
    Grok grok = grokCompiler.compile("%{COMBINEDAPACHELOG}");
    Match grokMatch = grok.match(log);
    Map<String, Object> capture = grokMatch.capture();
    SimpleDateFormat format = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH);
    return new Log((String) capture.get("request"), (String) capture.get("agent"),
        (String) capture.get("auth"), (String) capture.get("ident"),
        (String) capture.get("verb"), (String) capture.get("referrer"),
        Integer.parseInt((String) capture.get("response")),
        Integer.parseInt((String) capture.get("bytes")),
        (String) capture.get("clientip"), (String) capture.get("httpversion"),
        (String) capture.get("rawrequest"), format.parse((String) capture.get("timestamp")));
  }

  public Date getTimestamp() {
    return timestamp;
  }

  public int getBytes() {
    return bytes;
  }

  public int getResponse() {
    return response;
  }

  public String getAgent() {
    return agent;
  }

  public String getAuth() {
    return auth;
  }

  public String getClientIP() {
    return clientIP;
  }

  public String getHttpVersion() {
    return httpVersion;
  }

  public String getIdent() {
    return ident;
  }

  public String getRawRequest() {
    return rawRequest;
  }

  public String getReferrer() {
    return referrer;
  }

  public String getRequest() {
    return request;
  }

  public String getVerb() {
    return verb;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Log log = (Log) o;
    return response == log.response && bytes == log.bytes && Objects.equals(request, log.request) && Objects.equals(agent, log.agent) && Objects.equals(auth, log.auth) && Objects.equals(ident, log.ident) && Objects.equals(verb, log.verb) && Objects.equals(referrer, log.referrer) && Objects.equals(clientIP, log.clientIP) && Objects.equals(httpVersion, log.httpVersion) && Objects.equals(rawRequest, log.rawRequest) && Objects.equals(timestamp, log.timestamp);
  }

  @Override
  public int hashCode() {
    return Objects.hash(request, agent, auth, ident, verb, referrer, response, bytes, clientIP, httpVersion, rawRequest, timestamp);
  }

  @Override
  public String toString() {
    return "Log{" +
        "request='" + request + '\'' +
        ", agent='" + agent + '\'' +
        ", auth='" + auth + '\'' +
        ", ident='" + ident + '\'' +
        ", verb='" + verb + '\'' +
        ", referrer='" + referrer + '\'' +
        ", response=" + response +
        ", bytes=" + bytes +
        ", clientIP='" + clientIP + '\'' +
        ", httpVersion='" + httpVersion + '\'' +
        ", rawRequest='" + rawRequest + '\'' +
        ", timestamp=" + timestamp +
        '}';
  }
}
