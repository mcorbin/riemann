(ns riemann.reaper
  "A reaper is a task which periodically expires events from the core index."
  (:require [riemann.index :as index]
            [riemann.core :as core]
            [riemann.pubsub :as pubsub]
            [riemann.time :refer [every! unix-time]]
            [clojure.tools.logging :refer :all]))

(defn expire-events
  "Expires events from the core index."
  [core interval keep-keys]
  (when-let [i (:index core)]
    (doseq [state (index/expire i)]
      (try
        (let [e (-> (select-keys state keep-keys)
                    (merge {:state "expired"
                            :time (unix-time)}))]
          (when-let [registry (:pubsub core)]
            (pubsub/publish! registry "index" e))
          (core/stream! core e))
        (catch Exception e
          (warn e "Caught exception while processing expired events"))))))

(defn reaper
  "Create a task which expires events from its core's index every interval
  (default 10) seconds. Expired events are streamed to the core's streams. The
  streamed events have only the host and service copied, current time, and
  state expired. Expired events from the index are also published to the
  \"index\" pubsub channel.
  Options:
  :keep-keys A list of event keys which should be preserved from the indexed
             event in the expired event. Defaults to [:host :service], which
             means that when an event expires, its :host and :service are
             copied to a new event, but no other keys are preserved.
             The state of an expired event is always \"expired\", and its time
             is always the time that the event expired."
  ([[interval opts] core]
   (let [interval (or interval 10)
         keep-keys (get (or opts {}) :keep-keys [:host :service])]
     (every! interval
       (let [core @core]
         (fn []
           (expire-events core interval keep-keys)))))))
