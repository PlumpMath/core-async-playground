(ns core-async-playground.two-pc
  (:require [clojure.core.async :as async :refer [<! >! <!! >!! timeout chan alt! alts! alts!! go go-loop close! thread put!]]))

; refactoring for unit tests
; retry aborts
; add timeouts for write and prepare
; add values storage

(def coord (chan 100))
(def ss {:sys1 (chan 100) :sys2 (chan 100)})
(defn ss-ids [] (keys ss))

(def state {:sys1 (atom "init") :sys2 (atom "init")})
(defn print-state [] (str "STATE: " (vec (map (fn [[id st]] [id @st]) state))))

(def vv {:sys1 (atom 0) :sys2 (atom 0)})
(def vv-unc {:sys1 (atom nil) :sys2 (atom nil)})


(def log (chan 100))
(go-loop []
  (when-let [msg (<! log)]
    (println msg)
    (recur)))
(defn log! [msg] (put! log msg))
(defn log-state! [] (log! (print-state)))

(defn set-state! [id st]
  (do 
    (reset! (id state) st))
    (log-state!))

(defn check-state [st] (reduce #(and %1 %2) (map #(= @% st) (vals state))))
(defn ids-bad-state [st] (keys (filter #(not= @(second %) st) state)))

(defn send-cmd [ids cmd]
  (doseq [id ids]
    (>!! (id ss) [coord cmd])
    (log! (str "coord sent " cmd " to " id))
    (set-state! id (:cmd cmd))))

(defn receive-acks [ids]
  (doseq [_ ids]
    (let [[id st] (<!! coord)]
      (log! (str "coord received " st " from " id))
      (set-state! id st))))

(defn transact [value]
  (thread
    (log! "TRANSACTION STARTED")
    (log-state!)

    (send-cmd (ss-ids) {:cmd "write" :value value})
    (receive-acks (ss-ids))

    (if (check-state "write-ok")
      (do
        (log! "WRITE OK")

        (send-cmd (ss-ids) {:cmd "prepare"})
        (receive-acks (ss-ids))

        (if (check-state "prepare-ok")
          (do
            (log! "PREPARE OK")

            (while (not (check-state "commit-ok"))
              (let [ids (ids-bad-state "commit-ok")]
                (send-cmd ids {:cmd "commit"})
                (receive-acks ids)))

            (log! "COMMIT OK")
            "TRANSACTION SUCCEEDED")
          ; prepare not ok
          (do
            (send-cmd (ss-ids) {:cmd "abort"})
            (receive-acks (ss-ids))
            "TRANSACTION FAILED"))
      )
      ; write not ok
      (do
        (send-cmd (ss-ids) {:cmd "abort"})
        (receive-acks (ss-ids))
        "TRANSACTION FAILED"))
  ))

(defn receive-and-reply [id st]
  (go
    (let [[sender msg] (<! (id ss))]
      (log! (str id " received " msg))
      (>! sender [id st])
      (log! (str id " sent " st))
  ))
  "")

;;

(go 
  (log! (<! (transact 11)))
  (log-state!))
    
;;

(receive-and-reply :sys1 "write-ok")
(receive-and-reply :sys2 "write-ok")
(receive-and-reply :sys1 "prepare-ok")
(receive-and-reply :sys2 "prepare-ok")
(receive-and-reply :sys1 "commit-ok")
(receive-and-reply :sys2 "commit-ok")

;;

(receive-and-reply :sys1 "write-ok")
(receive-and-reply :sys2 "write-not-ok")
(receive-and-reply :sys1 "abort-ok")
(receive-and-reply :sys2 "abort-ok")


