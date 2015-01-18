(ns core-async-playground.core
  (:require [clojure.core.async :as async :refer [<! >! <!! >!! timeout chan alt! alts! alts!! go close! thread]]))

; Snack machine that can give you one defined snack for cost
(defn snack-machine [snack-name cost]
  (let [money (chan)
        snacks (chan)]
    (go (loop [money-received 0]
          (cond (>= money-received cost)
            (do
              (>! snacks snack-name)
              (recur (- money-received cost)))
            :else (recur (+ money-received (<! money))))))
    [money snacks]))

(defn put-money!! [[money _] amount]
  (first (alts!! [[money amount] (timeout 1000)])))

(defn get-snack!! [[_ snacks]]
  (first (alts!! [snacks (timeout 1000)])))
