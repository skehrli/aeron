CHECKER_HOME="/home/sascha/Documents/uni/research/plse/cf-upstream/checker"
 ASM_JARS="lib/asm-9.7.jar:lib/asm-commons-9.7.jar:lib/asm-util-9.7.jar"
 PROJECT_CP="aeron-test-support/build/libs/aeron-test-support-1.49.0-SNAPSHOT.jar:/usr/lib/jvm/default/lib/tools.jar:$CHECKER_HOME/dist/checker.jar"

 # Get the full main+test classpath from our updated Gradle task
 AGGREGATED_CLASSPATH=$(./gradlew -q printAggregatedClasspath)

 # The final classpath, with the ASM fix prepended
 FINAL_CLASSPATH="$ASM_JARS:$PROJECT_CP:$AGGREGATED_CLASSPATH"
 echo "$FINAL_CLASSPATH" | tr ':' '\n' | grep asm

 # Find sources in main, test, AND generated-sources directories
 find aeron-test-support/src/main -name "*.java" -exec \
   "$CHECKER_HOME/bin/infer-and-annotate.sh" \
   "ResourceLeakChecker" \
   "$FINAL_CLASSPATH" \
   {} +
