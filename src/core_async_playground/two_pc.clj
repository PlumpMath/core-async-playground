(ns core-async-playground.two-pc
  (:require [clojure.core.async :as async :refer [<! >! <!! >!! timeout chan alt! alts! alts!! go go-loop close! thread put!]]))

; retry aborts
; add timeouts for write and prepare
; add values storage

(defn two-pc []
  (let [coord (chan 100)
        sys1 (chan 100)
        sys2 (chan 100)
        log (chan 100)
        log-fn #(put! log %)
        result     
        [
          {:chan coord
           :ss {:sys1 sys1 :sys2 sys2}
           :state {:sys1 (atom "init") :sys2 (atom "init")}
           :log log-fn
          }
          {:id :sys1
           :chan sys1
           :value (atom 0)
           :value-unc (atom nil)
           :log log-fn}
          {:id :sys2
           :chan sys2
           :value (atom 0)
           :value-unc (atom nil)
           :log log-fn}
        ]]
    (go-loop []
      (when-let [msg (<! log)]
        (println "## " msg)
        (recur)))
    (log-fn "2PC initailized")
    result
  ))

(defn- print-state [coord]
  (str "STATE: " (vec (map (fn [[id st]] [id @st]) (:state coord)))))
(defn- log [log-holder msg]
  ((:log log-holder) msg))
(defn- log-state [coord]
  (log coord (print-state coord)))

(defn- set-state! [coord id st] 
  (reset! (id (:state coord)) st)
  (log-state coord))
(defn- check-state [coord st]
  (reduce #(and %1 %2) (map #(= @% st) (vals (:state coord)))))
(defn- ids-bad-state [coord st]
  (keys (filter #(not= @(second %) st) (:state coord))))

(defn- send-cmd [coord ids cmd]
  (doseq [id ids]
    (>!! (id (:ss coord)) [(:chan coord) cmd])
    (log coord (str "coord sent " cmd " to " id))
    (set-state! coord id (:cmd cmd))))

(defn- receive-acks [coord ids]
  (doseq [_ ids]
    (let [[id st] (<!! (:chan coord))]
      (log coord (str "coord received " st " from " id))
      (set-state! coord id st))))

(defn transact [coord value]
  (thread
    (log coord (str "TRANSACTION STARTED for value " value))
    (log-state coord)

    (send-cmd coord (keys (:ss coord)) {:cmd "write" :value value})
    (receive-acks coord (keys (:ss coord)))

    (if (check-state coord "write-ok")
      (do
        (log coord "WRITE OK")

        (send-cmd coord (keys (:ss coord)) {:cmd "prepare"})
        (receive-acks coord (keys (:ss coord)))

        (if (check-state coord "prepare-ok")
          (do
            (log coord "PREPARE OK")

            (while (not (check-state coord "commit-ok"))
              (let [ids (ids-bad-state coord "commit-ok")]
                (send-cmd coord ids {:cmd "commit"})
                (receive-acks coord ids)))

            (log coord "COMMIT OK")
            "TRANSACTION SUCCEEDED")
          ; prepare not ok
          (do
            (send-cmd coord (keys (:ss coord)) {:cmd "abort"})
            (receive-acks coord (keys (:ss coord)))
            "TRANSACTION FAILED"))
      )
      ; write not ok
      (do
        (send-cmd coord (keys (:ss coord)) {:cmd "abort"})
        (receive-acks coord (keys (:ss coord)))
        "TRANSACTION FAILED"))
  ))

(defn receive-and-reply [sys st]
  (go
    (let [[sender msg] (<! (:chan sys))
          id (:id sys)]
      (log sys (str id " received " msg))
      (>! sender [id st])
      (log sys (str id " sent " st))
  )))

;;;

(let [[coord sys1 sys2] (two-pc)]
  (go 
    (log coord (<! (transact coord 11)))
    (log-state coord))
  (receive-and-reply sys1 "write-ok")
  (receive-and-reply sys2 "write-ok")
  (receive-and-reply sys1 "prepare-ok")
  (receive-and-reply sys2 "prepare-ok")
  (receive-and-reply sys1 "commit-not-ok")
  (receive-and-reply sys2 "commit-ok")  
  (receive-and-reply sys1 "commit-ok")
  ""
)
  
(let [[coord sys1 sys2] (two-pc)]
  (go 
    (log coord (<! (transact coord 11)))
    (log-state coord))
  (receive-and-reply sys1 "write-ok")
  (receive-and-reply sys2 "write-not-ok")
  (receive-and-reply sys1 "abort-ok")
  (receive-and-reply sys2 "abort-ok")
  ""
)
