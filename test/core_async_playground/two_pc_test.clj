(ns core-async-playground.two-pc-test
  (:require [clojure.test :refer :all]
            [core-async-playground.two-pc :refer :all]
            [clojure.core.async :as async :refer [<! >! <!! >!! timeout chan alt! alts! alts!! go close! thread]]))

(defn <!!-timeout [ch] (first (alts!! [ch (timeout 100)])))
(defn assert-value [sys value value-unc]
  (is (= value @(:value sys)))
  (is (= value-unc @(:value-unc sys))))
(defn setup-transact [coord value test-chan]
  (go 
    (let [result (<! (transact coord value))]
      (>! test-chan result)
      (log coord result)
      (log-state coord))))


(deftest success
  (let [[coord sys1 sys2] (two-pc)
        test-chan (chan 100)]
    (log coord "\n SUCCESS CASE")
    (setup-transact coord 11 test-chan)

    (assert-value sys1 nil nil)
    (is (= {:cmd "write" :value 11} (receive-and-reply sys1 "write-ok")))
    (assert-value sys1 nil {:value 11 :state "write-ok"})
    (is (= {:cmd "write" :value 11} (receive-and-reply sys2 "write-ok")))
    (is (= {:cmd "prepare"} (receive-and-reply sys1 "prepare-ok")))
    (assert-value sys1 nil {:value 11 :state "prepare-ok"})
    (is (= {:cmd "prepare"} (receive-and-reply sys2 "prepare-ok")))
    (is (= {:cmd "commit"} (receive-and-reply sys1 "commit-ok")))
    (assert-value sys1 11 nil)
    (is (= {:cmd "commit"} (receive-and-reply sys2 "commit-ok")))
    (is (= "TRANSACTION SUCCEEDED" (<!!-timeout test-chan)))
    (is (= {:sys1 "commit-ok" :sys2 "commit-ok"} (state coord)))
  ))

(deftest commit-retry
  (let [[coord sys1 sys2] (two-pc)
        test-chan (chan 100)]
    (log coord "\n COMMIT RETRY")
    (setup-transact coord 11 test-chan)

    (is (= {:cmd "write" :value 11} (receive-and-reply sys1 "write-ok")))
    (is (= {:cmd "write" :value 11} (receive-and-reply sys2 "write-ok")))
    (is (= {:cmd "prepare"} (receive-and-reply sys1 "prepare-ok")))
    (is (= {:cmd "prepare"} (receive-and-reply sys2 "prepare-ok")))
    (is (= {:cmd "commit"} (receive-and-reply sys1 "commit-not-ok")))
    (assert-value sys1 nil {:value 11 :state "prepare-ok"})
    (is (= {:cmd "commit"} (receive-and-reply sys2 "commit-ok")))
    (is (= {:cmd "commit"} (receive-and-reply sys1 "commit-not-ok")))
    (assert-value sys1 nil {:value 11 :state "prepare-ok"})
    (is (= {:cmd "commit"} (receive-and-reply sys1 "commit-ok")))
    (assert-value sys1 11 nil)
    (is (= "TRANSACTION SUCCEEDED" (<!!-timeout test-chan)))
    (is (= {:sys1 "commit-ok" :sys2 "commit-ok"} (state coord)))
  ))

(deftest write-abort
  (let [[coord sys1 sys2] (two-pc)
        test-chan (chan 100)]
    (log coord "\n WRITE ABORT")
    (setup-transact coord 11 test-chan)

    (is (= {:cmd "write" :value 11} (receive-and-reply sys1 "write-ok")))
    (assert-value sys1 nil {:value 11 :state "write-ok"})
    (is (= {:cmd "write" :value 11} (receive-and-reply sys2 "write-not-ok")))
    (assert-value sys2 nil nil)
    (is (= {:cmd "abort"} (receive-and-reply sys1 "abort-ok")))
    (assert-value sys1 nil nil)
    (is (= {:cmd "abort"} (receive-and-reply sys2 "abort-ok")))
    (assert-value sys2 nil nil)
    (is (= "TRANSACTION FAILED" (<!!-timeout test-chan)))
    (is (= {:sys1 "abort-ok" :sys2 "abort-ok"} (state coord)))
  ))

(deftest prepare-abort
  (let [[coord sys1 sys2] (two-pc)
        test-chan (chan 100)]
    (log coord "\n PREPARE ABORT")
    (setup-transact coord 11 test-chan)

    (is (= {:cmd "write" :value 11} (receive-and-reply sys1 "write-ok")))
    (is (= {:cmd "write" :value 11} (receive-and-reply sys2 "write-ok")))
    (is (= {:cmd "prepare"} (receive-and-reply sys1 "prepare-ok")))
    (assert-value sys1 nil {:value 11 :state "prepare-ok"})
    (is (= {:cmd "prepare"} (receive-and-reply sys2 "prepare-not-ok")))
    (assert-value sys2 nil nil)
    (is (= {:cmd "abort"} (receive-and-reply sys1 "abort-ok")))
    (assert-value sys1 nil nil)
    (is (= {:cmd "abort"} (receive-and-reply sys2 "abort-ok")))
    (assert-value sys2 nil nil)
    (is (= "TRANSACTION FAILED" (<!!-timeout test-chan)))
    (is (= {:sys1 "abort-ok" :sys2 "abort-ok"} (state coord)))
  ))

(deftest receive-and-reply-receive-timeout-test
  (let [[coord sys1 sys2] (two-pc)
        test-chan (chan)]
    (is (nil? (receive-and-reply sys1 "write-ok")))
  ))

(deftest receive-and-reply-reply-timeout-test
  (let [[coord sys1 sys2] (two-pc)
        test-chan (chan)]
    (>!! (:chan sys1) [123 test-chan]) ; put to test-chan blocks
    (is (nil? (receive-and-reply sys1 "write-ok")))
  ))
