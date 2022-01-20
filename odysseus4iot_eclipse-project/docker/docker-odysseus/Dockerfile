FROM odysseusol/odysseus
ADD ./de.uniol.inf.is.odysseus.activityclassify.feature.update /de.uniol.inf.is.odysseus.activityclassify.feature.update
RUN /usr/lib/odysseus/odysseus -application org.eclipse.equinox.p2.director -repository file:///de.uniol.inf.is.odysseus.activityclassify.feature.update -installIU de.uniol.inf.is.odysseus.activityclassify.feature.feature.group -destination /usr/lib/odysseus -profile DefaultProfile;
RUN /usr/lib/odysseus/odysseus -application org.eclipse.equinox.p2.director -repository https://odysseus.informatik.uni-oldenburg.de/updatesite/odysseus_all_new/stable/origin/master/latest/ -installIU de.uniol.inf.is.odysseus.database.feature.feature.group -destination /usr/lib/odysseus -profile DefaultProfile;
EXPOSE 8888
CMD ["/usr/lib/odysseus/odysseus","-console -debug -Dio.netty.leakDetection.level=advanced -data @noDefault"]