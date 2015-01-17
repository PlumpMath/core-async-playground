(ns core-async-playground.core
  (:require [clojure.core.async :as async :refer [<! >! <!! >!! timeout chan alt! alts! alts!! go close! thread]]))

; Snack machine that can give you one defined snack for cost
(defn snack-machine [snack-name cost]
  (let [money (chan)
        snacks (chan)]
    (go (loop [remaining-sum cost]
          (let [pennies (<! money)]
            (cond (>= pennies remaining-sum)
              (do
                (>! snacks snack-name)
                (recur cost))
              :else (recur (- remaining-sum pennies))))))
    [money snacks]))

(defn put-money!! [[money _] amount]
  (>!! money amount))

(defn get-snack!! [[_ snacks]]
  (first (alts!! [snacks (timeout 1000)])))
