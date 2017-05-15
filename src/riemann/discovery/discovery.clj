(ns riemann.discovery.discovery
  (:require [riemann.time :as time]
            [riemann.config :refer [reinject core]]
            [riemann.index :refer [insert delete]]
            [riemann.streams :refer [expired?
                                     where
                                     tagged]]))

(defprotocol Discovery
  (initialize [this discovery-config global-config]
    "Lookup and update state. Should returns config-vec
     discovery-config is a map containing the configuration for the
     discovery mechanism.
     global-discovery is a map containing the configuration shared between
     discovery mechanism"))

(defn get-service-map
  "Takes a service and a default ttl, generate a service map"
  [service default-ttl]
  (reduce #(assoc %1 [%2 (:name service)] {:tags ["riemann-discovery"]
                                           :time (time/unix-time)
                                           :ttl (or (:ttl service) default-ttl)})
          {}
          (:hosts service [nil])))

(defn get-services-from-configuration-elem
  "takes a part of a configuration (a map containing the :ttl and :service keys) and generates a map containing all services"
  [config-elem]
  (reduce #(merge %1 (get-service-map %2 (:ttl config-elem))) {}
          (:services config-elem)))

(defn get-services-from-configuration
  "Takes a configuration (a vector of maps), generate a map containing all services"
  [config]
  (reduce #(merge %1 (get-services-from-configuration-elem %2)) {} config))

(defn generate-events
  "takes a list of services and generates a list of events"
  [services state]
  (map (fn [[[host service] {ttl :ttl time :time}]]
         {:host host
          :service service
          :time time
          :tags ["riemann-discovery"]
          :state state
          :ttl ttl}) services))

(defn reinject-events
  "reinject events into Riemann"
  [events]
  (doseq [event events]
    (reinject event)))

(defn get-new-state
  "takes the current and the new state, reinject events, returns the next state"
  [current-state new-state]
  (let [current-state-set (set (keys current-state))
        new-state-set (set (keys new-state))
        ;; services removed in the new state
        removed-services (->> (clojure.set/difference current-state-set
                                                           new-state-set)
                                   (select-keys current-state))
        ;; services added in the new state
        added-services (->> (clojure.set/difference new-state-set
                                                         current-state-set)
                            (select-keys new-state))
        ;; keys for services common services between the current and the next state
        common-services-keys (clojure.set/intersection current-state-set
                                                       new-state-set)
        ;; updates-services are common services that need to emitted
        ;; (because they are expired)
        ;; old-services are common services that need to be present in the next state
        ;; because they are not expired
        [updated-services old-services]
        (reduce (fn [result k]
                  ;; multiply by 2 the ttl to give a chance to detect
                  ;; a missing service
                  (if (expired? (update (get current-state k) :ttl * 2))
                    (update result 0 #(assoc % k (get new-state k)))
                    (update result 1 #(assoc % k (get current-state k)))))
                [{} {}] common-services-keys)
        ;; the next state returned by the fn
        result-state (merge updated-services old-services added-services)
        ;; we should emit these events
        events (concat (generate-events updated-services "added")
                       (generate-events added-services "added")
                       (generate-events removed-services "removed"))]
    ;; reinject events
    (reinject-events events)
    ;; returns the next state
    result-state))

(defn discovery-stream
  "You can use this stream to automatically index/remove events emitted by riemann-discovery"
  [index]
  (where (tagged "riemann-discovery")
    (fn [event]
      (let [event (update event :service #(str "discovery-" %))]
        (cond
          (= "added" (:state event)) (index event)
          (= "removed" (:state event)) (index event))))))
