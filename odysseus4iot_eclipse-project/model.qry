SELECT model_title, features_json_content, list_of_predicted_classes, resampled_rate_in_hz, algorithm, list_of_functions, list_of_axes, window_size, window_stride, accuracy_test, f1_test
FROM public.experiment_result
WHERE model_title <> ''
AND algorithm LIKE '%Random%Forest%'
AND window_size = 5000
AND window_stride = '100%'
AND list_of_axes = 'gyrMag_accMag'
AND accuracy_test >= 0.9
AND f1_test >= 0.9
AND (model_title = '3Cattle1FarmRF_Lying' OR model_title = '3Cattle1FarmRF_Grassing' OR model_title = '3Cattle1FarmRF_Ruminating');