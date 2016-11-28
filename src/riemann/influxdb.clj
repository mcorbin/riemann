(ns riemann.influxdb
  "Forwards events to InfluxDB. Supports InfluxDB 0.9 or Higher"
  (:require
    [clojure.set :as set])
  (:import
   (java.util.concurrent TimeUnit)
   (javax.net.ssl SSLContext X509TrustManager HostnameVerifier)
   (java.security SecureRandom)
   (java.security.cert X509Certificate)
   (org.influxdb InfluxDBFactory InfluxDB$ConsistencyLevel)
   (org.influxdb.dto BatchPoints Point)))


(defn nil-or-empty-str
  [s]
  (or (nil? s) (= "" s)))

;; specific influxdb-deprecated code
(def special-fields
  "A set of event fields in Riemann with special handling logic."
  #{:host :service :time :metric :tags :ttl :precision})

;; specific influxdb-deprecated code
(defn event-tags
  "Generates a map of InfluxDB tags from a Riemann event. Any fields in the
  event which are named in `tag-fields` will be converted to a string key/value
  entry in the tag map."
  [tag-fields event]
  (->> (select-keys event tag-fields)
       (remove (fn [[k v]] (or (nil? v) (= "" v))))
       (map #(vector (name (key %)) (str (val %))))
       (into {})))

;; specific influxdb-deprecated code
(defn event-fields
  "Generates a map of InfluxDB fields from a Riemann event. The event's
  `metric` is converted to the `value` field, and any additional event fields
  which are not standard Riemann properties or in `tag-fields` will also be
  present."
  [tag-fields event]
  (let [ignored-fields (set/union special-fields tag-fields)]
    (-> event
        (->> (remove (comp ignored-fields key))
             (remove (fn [[k v]] (or (nil? v) (= "" v))))
             (map #(vector (name (key %)) (val %)))
             (into {}))
        (assoc "value" (:metric event)))))

(defn get-trust-manager
  []
  (let [trust-manager (proxy [X509TrustManager] []
                        (checkServerTrusted [_ _])
                        (checkClientTrusted [_ _] )
                        (getAcceptedIssuers [] (make-array X509Certificate 0)))]
    (into-array (list trust-manager))))

(defn get-ssl-factory
  "Get an instance of `javax.net.ssl.SSLSocketFactory`"
  []
  (let [ssl-context (SSLContext/getInstance "TLS")]
    (.init ssl-context nil (get-trust-manager) (new SecureRandom))
    (.getSocketFactory ssl-context)))

(defn get-hostname-verifier
  "Get an instance of `javax.net.ssl.HostnameVerifier`"
  []
  (let [verifier (proxy [HostnameVerifier] []
                   (verify [_ _] true))]
    verifier))

(defn get-builder
  "Returns new okhttp3.OkHttpClient$Builder"
  [{:keys [timeout insecure]}]
  (let [builder (new okhttp3.OkHttpClient$Builder)]
    (when insecure
      (.sslSocketFactory builder (get-ssl-factory))
      (.hostnameVerifier builder (get-hostname-verifier)))
    (doto builder
      (.readTimeout timeout TimeUnit/MILLISECONDS)
      (.writeTimeout timeout TimeUnit/MILLISECONDS)
      (.connectTimeout timeout TimeUnit/MILLISECONDS))))

(defn get-client
  "Returns an `org.influxdb.InfluxDB` instance"
  [{:keys [scheme host port username password] :as opts}]
  (let [url (str scheme "://" host ":" port)]
    (InfluxDBFactory/connect url username password (get-builder opts))))

(defn get-batchpoint
  "Returns a `org.influxdb.dto.BatchPoints` instance"
  [{:keys [tags db retention consistency]}]
  (let [builder (doto (BatchPoints/database db)
                      (.consistency (InfluxDB$ConsistencyLevel/valueOf consistency)))]
    (when retention (.retentionPolicy builder retention))
    (doseq [[k v] tags] (.tag builder (name k) v))
    (.build builder)))

(defn get-time-unit
  "returns a value from the TimeUnit enum depending of the `precision` parameters.
  The `precision` parameter is a keyword whose possibles values are `:seconds`, `:milliseconds` and `:microseconds`.
  Returns `TimeUnit/SECONDS` by default"
  [precision]
  (cond
    (= precision :milliseconds) TimeUnit/MILLISECONDS
    (= precision :microseconds) TimeUnit/MICROSECONDS
    (= precision :seconds) TimeUnit/SECONDS
    true TimeUnit/SECONDS))

(defn convert-time
  "Converts the `time-event` parameter (which is time second) in a new time unit specified by the `precision` parameter. It also converts the time to long.
  The `precision` parameter is a keyword whose possibles values are `:seconds`, `:milliseconds` and `:microseconds`.
  Returns time in seconds by default"
  [time-event precision]
  (cond
    (= precision :milliseconds) (long (* 1000 time-event))
    (= precision :microseconds) (long (* 1000000 time-event))
    (= precision :seconds) (long time-event)
    true (long time-event)))

;; specific influxdb-deprecated code
(defn event->point-9
  "Converts a Riemann event into an InfluxDB Point (an instance of `org.influxdb.dto.Point`.
  The first parameter is the event. The `:precision` key of the event is used to converts the event `time` into the correct time unit (default seconds).
  The second parameter is the option map passed to the influxdb stream."
  [event opts]
  (when (and (:time event) (:service event) (:metric event))
    (let [precision (:precision event :seconds) ;; use seconds default
          builder (doto (Point/measurement (:service event))
                        (.time (convert-time (:time event) precision) (get-time-unit precision)))
          tag-fields (set/union (:tag-fields opts) (:tag-fields event))
          tags   (event-tags (:tag-fields opts) event)
          event  (dissoc event :tag-fields) ;; remove :tag-fields from event
          fields (event-fields (:tag-fields opts) event)]
      (doseq [[k v] tags] (.tag builder k v))
      (doseq [[k v] fields] (.field builder k v))
      (.build builder))))

(defn event->point
  "Converts a Riemann event into an InfluxDB Point (an instance of `org.influxdb.dto.Point`.
  The first parameter is the event.
  The `:precision` event key is used to converts the event `time` into the correct time unit (default seconds).
  The `:measurement` event key is the influxdb measurement.
  The `:tags` event key contains all the Influxdb tags.
  The `:fields` event key contains all the Influxdb fields.
  The second parameter is the option map passed to the influxdb stream."
  [event]
  (when (and (:time event) (:measurement event))
    (let [precision (:precision event :seconds) ;; use seconds default
          builder (doto (Point/measurement (:measurement event))
                        (.time (convert-time (:time event) precision) (get-time-unit precision)))
          tags   (:tags event)
          fields (:fields event)]
      (doseq [[k v] tags] (when-not (nil-or-empty-str v) (.tag builder (name k) v)))
      (doseq [[k v] fields] (when-not (nil-or-empty-str v) (.field builder (name k) v)))
      (.build builder))))

(def default-opts
  "Default influxdb options"
  {:db "riemann"
   :scheme "http"
   :version :0.9
   :host "localhost"
   :username "root"
   :port 8086
   :tags {}
   :tag-fields #{}
   :consistency "ONE"
   :timeout 5000
   :insecure false})

(defn write-batch-point
  [connection batch-point]
  (.write connection batch-point))

(defn get-batchpoints
  [opts events]
  (map
   (fn [events]
     (when (not-empty events)
       (let [event (first events)
             opts (merge opts
                         (select-keys event [:db :retention :consistency]))
             batch-point (get-batchpoint opts)
             _ (doseq [event events] (.point batch-point (event->point event)))]
         batch-point
         )))
   events))

(defn influxdb-new-stream
  "Returns a function which accepts an event, or sequence of events, and writes
  them to InfluxDB.

  streams receive an event or a list of events. Each event can have these keys :
  `measurement`     The influxdb measurement.
  `tags`            A map of influxdb tags. Exemple : `{:foo \"bar\"}`
  `fields`          A map of influxdb fields. Exemple : `{:bar \"baz\"}`
  `precision`       The time precision. Possibles values are `:seconds`, `:milliseconds` and `:microseconds` (default `:seconds`). The event `time` will be converted.
  `:db`             Name of the database to write to. (optional)
  `:retention`      Name of retention policy to use. (optional)
  `:consistency`    The InfluxDB consistency level (default: `\"ONE\"`). Possibles values are ALL, ANY, ONE, QUORUM.
"
  [opts]
  (let [opts (merge default-opts opts)
        connection (get-client opts)]
    (fn streams
      [events]
      (let [events-partition (->> (if (sequential? events) events (list events))
                                  (partition-by #(:keys [:db :retention :consistency] %)))
            batch-points (get-batchpoints opts events-partition)]
        (doseq [batch-point batch-points] (write-batch-point connection batch-point))
        batch-points))))

(defn influxdb-deprecated
  "Returns a function which accepts an event, or sequence of events, and writes
  them to InfluxDB.

  influxdb-deprecated specifics options :
  `:tag-fields`     A set of event fields to map into InfluxDB series tags.
                    (default: `#{:host}`)
"
  [opts]
  (let [opts (merge default-opts opts)
        connection (get-client opts)]
    (fn streams
      [events]
      (let [events (->> (if (sequential? events) events (list events))
                        (keep #(event->point-9 % opts)))
            batch-point (get-batchpoint opts)
            _ (doseq [event events] (.point batch-point event))]
        (write-batch-point connection batch-point)
        batch-point))))

(defn influxdb
  "Returns a function which accepts an event, or sequence of events, and writes
  them to InfluxDB as a batch of measurement points. For performance, you should
  wrap this stream with `batch` or an asynchronous queue.
  Support InfluxdbDB 0.9 and higher.
  (influxdb {:host \"influxdb.example.com\"
             :db \"my_db\"
             :user \"riemann\"
             :password \"secret\"})
  General Options:
  `:db`             Name of the database to write to. (default: `\"riemann\"`)
  `:version`        Version of InfluxDB client to use. (default: `\":0.9\"`)
  `:scheme`         URL scheme for endpoint. (default: `\"http\"`)
  `:host`           Hostname to write points to. (default: `\"localhost\"`)
  `:port`           API port number. (default: `8086`)
  `:username`       Database user to authenticate as. (default: `\"root\"`)
  `:password`       Password to authenticate with. (optional)
  `:tags`           A common map of tags to apply to all points. (optional)
  `:retention`      Name of retention policy to use. (optional)
  `:timeout`        HTTP timeout in milliseconds. (default: `5000`)
  `:consistency`    The InfluxDB consistency level (default: `\"ONE\"`). Possibles values are ALL, ANY, ONE, QUORUM.
  `:insecure`       If scheme is https and certficate is self-signed. (optional)

  See `influxdb-deprecated` and `influxdb-low-level` for version-specific options.
"
  [opts]
  (let [opts (merge default-opts opts)]
    (if (= :new-stream (:version opts))
      (influxdb-new-stream opts)
      (influxdb-deprecated opts)
      )))
