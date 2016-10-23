(ns core-async-playground.two-pc-test
  (:require [clojure.test :refer :all]
            [core-async-playground.two-pc :refer :all]
            [clojure.core.async :as async :refer [<! >! <!! >!! timeout chan alt! alts! alts!! go close! thread]]))

(deftest success
  (let [[coord sys1 sys2] (two-pc)
        test-chan (chan 100)]
    (log coord "\n SUCCESS CASE")
    (go 
      (let [result (<! (transact coord 11))]
        (>! test-chan result)
        (log coord result)
        (log-state coord)))
    (receive-and-reply sys1 "write-ok")
    (receive-and-reply sys2 "write-ok")
    (receive-and-reply sys1 "prepare-ok")
    (receive-and-reply sys2 "prepare-ok")
    (receive-and-reply sys2 "commit-ok")  
    (receive-and-reply sys1 "commit-ok")
    (is (= "TRANSACTION SUCCEEDED" (<!! test-chan)))
    (is (= {:sys1 "commit-ok" :sys2 "commit-ok"} (state coord)))
  ))

(deftest prepare-abort
  (let [[coord sys1 sys2] (two-pc)
        test-chan (chan 100)]
    (log coord "\n PREPARE ABORT")
    (go 
      (let [result (<! (transact coord 11))]
        (>! test-chan result)
        (log coord result)
        (log-state coord)))
    (receive-and-reply sys1 "write-ok")
    (receive-and-reply sys2 "write-not-ok")
    (receive-and-reply sys1 "abort-ok")
    (receive-and-reply sys2 "abort-ok")
    (is (= "TRANSACTION FAILED" (<!! test-chan)))
    (is (= {:sys1 "abort-ok" :sys2 "abort-ok"} (state coord)))
  ))
