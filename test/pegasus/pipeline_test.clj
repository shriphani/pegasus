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

(deftype NotIdentityPipelineComponent []
  process/PipelineComponentProtocol

  (initialize
    [this config]
    (merge-with +
                config
                {:shit 1}))

  (run
    [this obj config]
    obj)

  (clean
    [this config]
    nil))

(deftype MalformedPipelineComponent []
  process/PipelineComponentProtocol

  (initialize
    [this config]
    nil)

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

          orig-config2 {:component1 (->NotIdentityPipelineComponent)
                        :component2 (->NotIdentityPipelineComponent)
                        :component3 (->NotIdentityPipelineComponent)
                        :component4 (->NotIdentityPipelineComponent)
                        :pipeline [[:component1 nil 1]
                                   [:component2 nil 1]
                                   [:component3 nil 1]
                                   [:component4 nil 1]]}

          final-config (process/initialize-component-configs orig-config)

          final-conf2  (process/initialize-component-configs orig-config2)]

      (is
       (= final-config
          orig-config))

      (is
       (not
        (= orig-config2
           final-conf2)))

      (-> final-conf2
          :shit
          (= 4)
          is)))

  (testing "Create a malformed initializer"
    (let [orig-config {:component1 (->MalformedPipelineComponent)
                       :component2 (->MalformedPipelineComponent)
                       :component3 (->MalformedPipelineComponent)
                       :component4 (->MalformedPipelineComponent)
                       :pipeline [[:component1 nil 1]
                                  [:component2 nil 1]
                                  [:component3 nil 1]
                                  [:component4 nil 1]]}]
      (is
       (thrown?
        Exception
        (process/initialize-component-configs orig-config))))))
