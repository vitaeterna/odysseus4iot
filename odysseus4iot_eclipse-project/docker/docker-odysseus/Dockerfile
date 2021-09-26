FROM odysseusol/odysseus
ADD ./prediction.update /prediction.update
#RUN chmod -R 777 /usr/lib/odysseus
RUN /usr/lib/odysseus/odysseus -application org.eclipse.equinox.p2.director -repository https://odysseus.informatik.uni-oldenburg.de/updatesite/odysseus_all_new/stable/origin/master/latest/ -installIU de.uniol.inf.is.odysseus.database.feature.feature.group -destination /usr/lib/odysseus -profile DefaultProfile;
#RUN /usr/lib/odysseus/odysseus -application org.eclipse.equinox.p2.director -repository http://localhost/prediction.update/ -installIU prediction.feature.group -destination /usr/lib/odysseus -profile DefaultProfile;
#RUN /usr/lib/odysseus/odysseus -application org.eclipse.equinox.p2.director -repository file:///prediction.update -installIU prediction.feature.group -destination /usr/lib/odysseus -profile DefaultProfile;
EXPOSE 8888
CMD ["/usr/lib/odysseus/odysseus","-console -debug -data @noDefault"]