(ns core-async-playground.two-pc
  (:require [clojure.core.async :as async :refer [<! >! <!! >!! timeout chan alt! alts! alts!! go go-loop close! thread put!]]))

(def coord (chan 100))
(def ss {:sys1 (chan 100) :sys2 (chan 100)})

(def state {:sys1 (atom "init") :sys2 (atom "init")})
(defn print-state [] (str "STATE: " (vec (map (fn [[id st]] [id @st]) state))))

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

(defn transact [value]
  (go
    (log! "TRANSACTION STARTED")
    (log-state!)

    ; send write
    (doseq [[id sys] ss]
      (>! sys value)
      (log! (str "coord sent WRITE " value " to " id))
      (set-state! id "write"))

    ; receive write acks
    (doseq [_ ss]
      (let [[id st] (<! coord)]
        (log! (str "coord received " st " from " id))
        (set-state! id st)))

    (if (check-state "write-ok")
      (do
        (log! "WRITE OK")

        ; send prepare
        (doseq [[id sys] ss]
          (>! sys "prepare")
          (log! (str "coord sent PREPARE to " id))
          (set-state! id "prepare"))

        ; receive prepape acks
        (doseq [_ ss]
          (let [[id st] (<! coord)]
            (log! (str "coord received " st " from " id))
            (set-state! id st)))

        (if (check-state "prepare-ok")
          (do
            (log! "PREPARE OK")

            ; send commit
            (doseq [[id sys] ss]
              (>! sys "prepare")
              (log! (str "coord sent COMMIT to " id))
              (set-state! id "commit"))

            ; receive commit acks
            (doseq [_ ss]
              (let [[id st] (<! coord)]
                (log! (str "coord received " st " from " id))
                (set-state! id st)))

            (if (check-state "commit-ok")
              (do (log! "COMMIT OK") true)
              false)
          )
          false)
      )
      false)
  ))

(defn receive-and-reply [id st]
  (go
    (log! (str id " received " (<! (id ss))))
    (>! coord [id st])
    (log! (str id " sent " st))
  ))

;;

(go 
  (if (<! (transact 11))
    (do (log! "TRANSACTION SUCCEEDED") (log-state!))
    (do (log! "TRANSACTION FAILED") (log-state!))))

(receive-and-reply :sys1 "write-ok")
(receive-and-reply :sys2 "write-ok")
(receive-and-reply :sys1 "prepare-ok")
(receive-and-reply :sys2 "prepare-ok")
(receive-and-reply :sys1 "commit-ok")
(receive-and-reply :sys2 "commit-ok")

