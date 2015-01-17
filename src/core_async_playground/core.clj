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

(def sm (snack-machine "Picnic" 30))
(def money (first sm))
(def snacks (second sm))
(>!! money 29)
(>!! money 1)
(alts!! [snacks (timeout 3000)])
(close! snacks)
(close! money)
