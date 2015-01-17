(ns core-async-playground.core-test
  (:require [clojure.test :refer :all]
            [core-async-playground.core :refer :all]
            [clojure.core.async :as async :refer [<! >! <!! >!! timeout chan alt! alts! alts!! go close! thread]]))

(deftest snack-machine-one-pennie
  (let [sm (snack-machine "Picnic" 30)]
    (put-money!! sm 30)
    (is (= "Picnic" (get-snack!! sm)))))

(deftest snack-machine-no-pennies
  (let [sm (snack-machine "Picnic" 30)]
    (is (= nil (get-snack!! sm)))))

(deftest snack-machine-two-pennies
  (let [sm (snack-machine "Picnic" 30)]
    (put-money!! sm 20)
    (is (= nil (get-snack!! sm)))
    (put-money!! sm 10)
    (is (= "Picnic" (get-snack!! sm)))))
