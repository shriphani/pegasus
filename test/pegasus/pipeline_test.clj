(ns pegasus.pipeline-test
  "Test the pipeline setup, execution and epilogue"
  (:require [clojure.test :refer :all]
            [pegasus.process :as process]))

(deftype IdentityPipelineComponent []
  process/PipelineComponentProtocol

  (initialize
    [this config]
    config)

  (run
    [this obj config]
    obj)

  (clean
    [this config]
    nil))

(deftest pipeline-test

  (testing "Create a pipeline - which initializes a blank config"
    (let [orig-config {:component1 (->IdentityPipelineComponent)
                       :component2 (->IdentityPipelineComponent)
                       :component3 (->IdentityPipelineComponent)
                       :component4 (->IdentityPipelineComponent)
                       :pipeline [[:component1 nil 1]
                                  [:component2 nil 1]
                                  [:component3 nil 1]
                                  [:component4 nil 1]]}

          final-config (process/initialize-component-configs orig-config)]

      (is
       (= final-config
          orig-config)))))
