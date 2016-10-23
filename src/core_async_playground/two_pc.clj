(ns core-async-playground.two-pc
  (:require
    [clojure.core.async :as async :refer [<! >! <!! >!! chan alt! alts! alts!! go go-loop close! thread put!]]
    [clojure.algo.generic.functor :refer (fmap)]
    [clojure.core.match :refer (match)]
  ))

; add state to sys
; retry aborts
; add timeouts for write and prepare
; think about other exceptional scenarios

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
           :value (ref nil)
           :value-unc (ref nil)
           :log log-fn}
          {:id :sys2
           :chan sys2
           :value (ref nil)
           :value-unc (ref nil)
           :log log-fn}
        ]]
    (go-loop []
      (when-let [msg (<! log)]
        (println "## " msg)
        (recur)))
    (log-fn "2PC initailized")
    result
  ))

(defn state [coord] (fmap deref (:state coord)))
(defn log [log-holder msg]
  ((:log log-holder) msg))
(defn log-state [coord]
  (log coord (str "state: " (state coord))))

(defn- set-state! [coord id st] 
  (reset! (id (:state coord)) st)
  (log-state coord))
(defn- check-state [coord st]
  (reduce #(and %1 %2) (map #(= @% st) (vals (:state coord)))))
(defn- ids-bad-state [coord st]
  (keys (filter #(not= @(second %) st) (:state coord))))

(defn- send-cmd [coord ids cmd]
  (doseq [id ids]
    (>!! (id (:ss coord)) [cmd (:chan coord)])
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

(defn receive-and-reply 
  ([sys st timeout]
    (first (alts!!
      [(go
        (let [[msg sender] (<! (:chan sys))
              id (:id sys)]
          (log sys (str id " received " msg))
          (match [msg st]
            [{:cmd "write" :value value} "write-ok"]
              (dosync (ref-set (:value-unc sys) {:value value :state "write-ok"}))
            [{:cmd "write" :value _} "write-not-ok"]
              nil 
            [{:cmd "prepare"} "prepare-ok"]
              (dosync (alter (:value-unc sys) #(merge % {:state "prepare-ok"})))
            [{:cmd "prepare"} "prepare-not-ok"]
              (dosync (ref-set (:value-unc sys) nil))
            [{:cmd "commit"} "commit-ok"]
              (dosync
                (ref-set (:value sys) (:value @(:value-unc sys)))
                (ref-set (:value-unc sys) nil))
            [{:cmd "commit"} "commit-not-ok"]
              nil
            [{:cmd "abort"} "abort-ok"]
              (dosync (ref-set (:value-unc sys) nil))
            :else
              (log sys (str "BAD TRANSITION: msg=" msg ", state=" st)))
          (log sys (str id " state: value=" @(:value sys) ", value-unc=" @(:value-unc sys)))
          (>! sender [id st])
          (log sys (str id " sent " st))
          msg))
      (async/timeout timeout)]))
  )
  ([sys st] (receive-and-reply sys st 100)))

