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
    (is (= {:cmd "write" :value 11} (receive-and-reply sys1 "write-ok")))
    (is (= {:cmd "write" :value 11} (receive-and-reply sys2 "write-ok")))
    (is (= {:cmd "prepare"} (receive-and-reply sys1 "prepare-ok")))
    (is (= {:cmd "prepare"} (receive-and-reply sys2 "prepare-ok")))
    (is (= {:cmd "commit"} (receive-and-reply sys1 "commit-ok")))
    (is (= {:cmd "commit"} (receive-and-reply sys2 "commit-ok")))
    (is (= "TRANSACTION SUCCEEDED" (<!! test-chan)))
    (is (= {:sys1 "commit-ok" :sys2 "commit-ok"} (state coord)))
  ))

(deftest commit-retry
  (let [[coord sys1 sys2] (two-pc)
        test-chan (chan 100)]
    (log coord "\n COMMIT RETRY")
    (go 
      (let [result (<! (transact coord 11))]
        (>! test-chan result)
        (log coord result)
        (log-state coord)))
    (is (= {:cmd "write" :value 11} (receive-and-reply sys1 "write-ok")))
    (is (= {:cmd "write" :value 11} (receive-and-reply sys2 "write-ok")))
    (is (= {:cmd "prepare"} (receive-and-reply sys1 "prepare-ok")))
    (is (= {:cmd "prepare"} (receive-and-reply sys2 "prepare-ok")))
    (is (= {:cmd "commit"} (receive-and-reply sys1 "commit-not-ok")))
    (is (= {:cmd "commit"} (receive-and-reply sys2 "commit-ok")))
    (is (= {:cmd "commit"} (receive-and-reply sys1 "commit-not-ok")))
    (is (= {:cmd "commit"} (receive-and-reply sys1 "commit-ok")))
    (is (= "TRANSACTION SUCCEEDED" (<!! test-chan)))
    (is (= {:sys1 "commit-ok" :sys2 "commit-ok"} (state coord)))
  ))

(deftest write-abort
  (let [[coord sys1 sys2] (two-pc)
        test-chan (chan 100)]
    (log coord "\n WRITE ABORT")
    (go 
      (let [result (<! (transact coord 11))]
        (>! test-chan result)
        (log coord result)
        (log-state coord)))
    (is (= {:cmd "write" :value 11} (receive-and-reply sys1 "write-ok")))
    (is (= {:cmd "write" :value 11} (receive-and-reply sys2 "write-not-ok")))
    (is (= {:cmd "abort"} (receive-and-reply sys1 "abort-ok")))
    (is (= {:cmd "abort"} (receive-and-reply sys2 "abort-ok")))
    (is (= "TRANSACTION FAILED" (<!! test-chan)))
    (is (= {:sys1 "abort-ok" :sys2 "abort-ok"} (state coord)))
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
    (is (= {:cmd "write" :value 11} (receive-and-reply sys1 "write-ok")))
    (is (= {:cmd "write" :value 11} (receive-and-reply sys2 "write-ok")))
    (is (= {:cmd "prepare"} (receive-and-reply sys1 "prepare-ok")))
    (is (= {:cmd "prepare"} (receive-and-reply sys2 "prepare-not-ok")))
    (is (= {:cmd "abort"} (receive-and-reply sys1 "abort-ok")))
    (is (= {:cmd "abort"} (receive-and-reply sys2 "abort-ok")))
    (is (= "TRANSACTION FAILED" (<!! test-chan)))
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
