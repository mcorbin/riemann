(ns riemann.influxdb-test
  (:require
    [clojure.test :refer :all]
    [riemann.influxdb :as influxdb]
    [riemann.logging :as logging]
    [riemann.test-utils :refer [with-mock]]
    [riemann.time :refer [unix-time]])
  (:import
   (java.util.concurrent TimeUnit)
   (org.influxdb InfluxDBFactory InfluxDB$ConsistencyLevel)
   (org.influxdb.dto BatchPoints Point))
  )

(logging/init)

(defn ^java.lang.reflect.Field get-field
  "Return Field object"
  [^Class class ^String field-name]
  (let [f (.getDeclaredField class field-name)]
    (.setAccessible f true)
    f))

(def measurement (get-field Point "measurement"))
(def time-field (get-field Point "time"))
(def tags (get-field Point "tags"))
(def fields (get-field Point "fields"))

; Would the next person working on influxdb kindly update these tests to use
; riemann.test-utils/with-mock? Would be nice to have something besides just
; integration tests. --Kyle, Sep 2015 :)

(deftest ^:influxdb ^:integration influxdb-test
  (testing "deprecated influxdb stream"
    (let [k (influxdb/influxdb
             {:host (or (System/getenv "INFLUXDB_HOST") "localhost")
              :db "riemann_test"})]
      (k {:host "riemann.local"
          :service "influxdb test"
          :state "ok"
          :description "all clear, uh, situation normal"
          :metric -2
          :time (unix-time)})
      (k {:service "influxdb test"
          :state "ok"
          :description "all clear, uh, situation normal"
          :metric 3.14159
          :time (unix-time)})
      (k {:host "no-service.riemann.local"
          :state "ok"
          :description "Missing service, not transmitted"
          :metric 4
          :time (unix-time)})))

  (testing "new influxdb stream"
    (let [k (influxdb/influxdb
             {:host (or (System/getenv "INFLUXDB_HOST") "localhost")
              :version :new-stream
              })]
      (k {:time 1428366765
          :tags {:foo "bar"
                 :bar "baz"}
          :precision :milliseconds
          :db "riemann_test"
          :measurement "measurement"
          :fields {:alice "bob"}})
      (k {:time 1428366765
          :tags {:foo "bar"
                 :bar "baz"}
          :precision :seconds
          :db "riemann_test"
          :consistency "ALL"
          :retention "autogen"
          :measurement "measurement"
          :fields {:alice "bob"}})
      ))
  )

(deftest event-fields
  (is (= (influxdb/event-fields
          #{}
          {:host "host-01"
           :service "test service"
           :time 1428366765
           :metric 42.08})
         {"value" 42.08}))
  (is (= (influxdb/event-fields
          #{}
          {:host "host-01"
           :service "test service"
           :time 1428366765
           :metric 42.08
           :hello "hello"})
         {"value" 42.08 "hello" "hello"}))
  (is (= (influxdb/event-fields
          #{:hello}
          {:host "host-01"
           :service "test service"
           :time 1428366765
           :metric 42.08
           :hello "hello"})
         {"value" 42.08})))

(deftest event-tags-test
  (is (= (influxdb/event-tags
          #{}
          {:host "host-01"
           :service "test service"
           :time 1428366765
           :metric 42.08})
         {}))
  (is (= (influxdb/event-tags
          #{:host}
          {:host "host-01"
           :service "test service"
           :time 1428366765
           :metric 42.08})
         {"host" "host-01"}))
  (is (= (influxdb/event-tags
          #{:host :hello}
          {:host "host-01"
           :service "test service"
           :time 1428366765
           :hello "hello"
           :metric 42.08})
         {"host" "host-01" "hello" "hello"})))

(deftest get-time-unit-test
  (is (= TimeUnit/SECONDS (influxdb/get-time-unit :seconds)))
  (is (= TimeUnit/MILLISECONDS (influxdb/get-time-unit :milliseconds)))
  (is (= TimeUnit/MICROSECONDS (influxdb/get-time-unit :microseconds)))
  (is (= TimeUnit/SECONDS (influxdb/get-time-unit :default))
      "Default value is SECONDS"))

(deftest convert-time-test
  (is (= 1 (influxdb/convert-time 1 :seconds))
      "seconds -> seconds")
  (is (= 1000 (influxdb/convert-time 1 :milliseconds))
      "seconds -> milliseconds")
  (is (= 1000000 (influxdb/convert-time 1 :microseconds))
      "seconds -> microseconds")
  (is (= 1 (influxdb/convert-time 1 :default))
      "seconds -> seconds (default)"))

(deftest point-conversion-deprecated
  (is (nil? (influxdb/event->point-9 {:service "foo test" :time 1} {:tag-fields #{}}))
      "Event with no metric is converted to nil")

  (testing "Minimal event is converted to point fields"
    (let [point (influxdb/event->point-9 {:host "host-01"
                                        :service "test service"
                                        :time 1428366765
                                        :metric 42.08}
                                       {:tag-fields #{:host}})]
      (is (= "test service" (.get measurement point)))
      (is (= 1428366765 (.get time-field point)))
      (is (= {"host" "host-01"} (into {} (.get tags point))))
      (is (= {"value" 42.08} (into {} (.get fields point))))))

  (testing "Event is converted with time in milliseconds"
    (let [point (influxdb/event->point-9 {:host "host-01"
                                        :service "test service"
                                        :time 1428366765
                                        :precision :milliseconds
                                        :metric 42.08}
                                       {:tag-fields #{:host}})]
      (is (= "test service" (.get measurement point)))
      (is (= 1428366765000 (.get time-field point)))
      (is (= {"host" "host-01"} (into {} (.get tags point))))
      (is (= {"value" 42.08} (into {} (.get fields point))))))

  (testing "Event is converted with time in microseconds"
    (let [point (influxdb/event->point-9 {:host "host-01"
                                        :service "test service"
                                        :time 1428366765
                                        :precision :microseconds
                                        :metric 42.08}
                                       {:tag-fields #{:host}})]
      (is (= "test service" (.get measurement point)))
      (is (= 1428366765000000 (.get time-field point)))
      (is (= {"host" "host-01"} (into {} (.get tags point))))
      (is (= {"value" 42.08} (into {} (.get fields point))))))

  (testing "Full event is converted to point fields"
    (let [point (influxdb/event->point-9 {:host "www-dev-app-01.sfo1.example.com"
                                        :service "service_api_req_latency"
                                        :time 1428354941
                                        :metric 0.8025
                                        :state "ok"
                                        :description "A text description!"
                                        :ttl 60
                                        :tags ["one" "two" "red"]
                                        :sys "www"
                                        :env "dev"
                                        :role "app"
                                        :loc "sfo1"
                                        :foo "frobble"}
                                       {:tag-fields #{:host :sys :env :role :loc}})]
      (is (= "service_api_req_latency" (.get measurement point)))
      (is (= 1428354941 (.get time-field point)))
      (is (= {"host" "www-dev-app-01.sfo1.example.com"
              "sys" "www"
              "env" "dev"
              "role" "app"
              "loc" "sfo1"}
             (into {} (.get tags point))))
      (is (= {"value" 0.8025
              "description" "A text description!"
              "state" "ok"
              "foo" "frobble"}
             (into {} (.get fields point))))))

  (testing ":sys and :loc tags and removed because nil or empty str. Same for :bar and :hello fields"
    (let [point (influxdb/event->point-9 {:host "www-dev-app-01.sfo1.example.com"
                                        :service "service_api_req_latency"
                                        :time 1428354941
                                        :metric 0.8025
                                        :state "ok"
                                        :description "A text description!"
                                        :ttl 60
                                        :tags ["one" "two" "red"]
                                        :sys nil
                                        :env "dev"
                                        :role "app"
                                        :loc ""
                                        :foo "frobble"
                                        :bar nil
                                        :hello ""}
                                       {:tag-fields #{:host :sys :env :role :loc}})]
      (is (= "service_api_req_latency" (.get measurement point)))
      (is (= 1428354941 (.get time-field point)))
      (is (= {"host" "www-dev-app-01.sfo1.example.com"
              "role" "app"
              "env" "dev"}
             (into {} (.get tags point))))
      (is (= {"value" 0.8025
              "description" "A text description!"
              "state" "ok"
              "foo" "frobble"}
             (into {} (.get fields point))))))

  (testing "event :tag-fields"
    (let [point (influxdb/event->point-9 {:host "host-01"
                                        :service "test service"
                                        :time 1428366765
                                        :precision :milliseconds
                                        :metric 42.08
                                        :env "dev"
                                        :tag-fields #{:env}}
                                       {:tag-fields #{:host}})]
      (is (= "test service" (.get measurement point)))
      (is (= 1428366765000 (.get time-field point)))
      (is (= {"host" "host-01"} (into {} (.get tags point))))
      (is (= {"value" 42.08 "env" "dev"} (into {} (.get fields point)))))))


(deftest point-conversion
  (is (nil? (influxdb/event->point {:service "foo test" :time 1}))
      "Event with no measurement is converted to nil")

  (testing "Minimal event is converted to point fields"
    (let [point (influxdb/event->point {:time 1428366765
                                        :tags {:foo "bar"
                                               :bar "baz"}
                                        :measurement "measurement"
                                        :fields {:alice "bob"}})]
      (is (= "measurement" (.get measurement point)))
      (is (= 1428366765 (.get time-field point)))
      (is (= {"alice" "bob"} (into {} (.get fields point))))))

  (testing "Event is converted with time in milliseconds"
    (let [point (influxdb/event->point {:time 1428366765
                                        :tags {:foo "bar"
                                               :bar "baz"}
                                        :precision :milliseconds
                                        :measurement "measurement"
                                        :fields {:alice "bob"}})]
      (is (= "measurement" (.get measurement point)))
      (is (= 1428366765000 (.get time-field point)))
      (is (= {"foo" "bar" "bar" "baz"} (into {} (.get tags point))))
      (is (= {"alice" "bob"} (into {} (.get fields point))))))

  (testing "Event is converted with time in microseconds"
    (let [point (influxdb/event->point {:time 1428366765
                                        :tags {:foo "bar"
                                               :bar "baz"}
                                        :precision :milliseconds
                                        :measurement "measurement"
                                        :fields {:alice "bob"}})]
      (is (= "measurement" (.get measurement point)))
      (is (= 1428366765000 (.get time-field point)))
      (is (= {"foo" "bar" "bar" "baz"} (into {} (.get tags point))))
      (is (= {"alice" "bob"} (into {} (.get fields point))))))

  (testing ":sys and :loc tags are removed because nil or empty str. Same for :bar and :hello fields"
    (let [point (influxdb/event->point {:time 1428366765
                                        :tags {:foo "bar"
                                               :bar "baz"
                                               :sys ""
                                               :loc nil}
                                        :precision :milliseconds
                                        :measurement "measurement"
                                        :fields {:alice "bob"
                                                 :bar nil
                                                 :hello ""}})]
      (is (= "measurement" (.get measurement point)))
      (is (= 1428366765000 (.get time-field point)))
      (is (= {"foo" "bar" "bar" "baz"} (into {} (.get tags point))))
      (is (= {"alice" "bob"} (into {} (.get fields point)))))))


{:time 1428366765
 :tags {:foo "bar"
        :bar "baz"
        :sys ""
        :loc nil}
 :precision :milliseconds
 :measurement "measurement"
 :fields {:alice "bob"
          :bar nil
          :hello ""}}
