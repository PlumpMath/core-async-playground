(ns core-async-playground.two-pc
  (:require [clojure.core.async :as async :refer [<! >! <!! >!! timeout chan alt! alts! alts!! go go-loop close! thread put!]]))

; refactoring, deduplication in `transact`
; add abort
; add retry of messages
; add values storage
; add timeouts for write and prepare

(def coord (chan 100))
(def ss {:sys1 (chan 100) :sys2 (chan 100)})

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

(defn send-cmd [cmd]
  (doseq [[id sys] ss]
    (>!! sys cmd)
    (log! (str "coord sent " cmd " to " id))
    (set-state! id (:cmd cmd))))

(defn receive-acks []
  (doseq [_ ss]
    (let [[id st] (<!! coord)]
      (log! (str "coord received " st " from " id))
      (set-state! id st))))

(defn transact [value]
  (thread
    (log! "TRANSACTION STARTED")
    (log-state!)

    ; send write
    (send-cmd {:cmd "write" :value value})
    ; receive write acks
    (receive-acks)

    (if (check-state "write-ok")
      (do
        (log! "WRITE OK")

        ; send prepare
        (send-cmd {:cmd "prepare"})
        ; receive prepape acks
        (receive-acks)

        (if (check-state "prepare-ok")
          (do
            (log! "PREPARE OK")

            ; send commit
            (send-cmd {:cmd "commit"})
            ; receive commit acks
            (receive-acks)

            (if (check-state "commit-ok")
              (do (log! "COMMIT OK") true)
              false))
          false)
      )
      ; write not ok
      (do
        ; send abort
        (send-cmd {:cmd "abort"})
        ; receive abort acks
        (receive-acks)
        false))
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


