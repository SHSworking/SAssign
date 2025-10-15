//Stores events detected by sensor (video camera for one, in future audio sensor, light sensor, ..s
//future expansion to rename to sensor_oid (from video_camera_oid)

case class Events(
                   geographical_location_oid: BigInt, //remove for expansion to different type of Sensor
                   video_camera_oid: BigInt,
                   detection_oid: BigInt, //unique
                   item_name: String,
                   // for varchar(5000), need to match keywords if detection algorithm output
                   // inconsistent descriptions for same type item
                   // [future use] detection algo output semantics e.g. "LUGGAGE green 350mm"
                   //   and require features extraction
                   //   current task assumption : preprocessing to object-type as per WOG Video Analytics metadata standards

                   timestamp_detected: BigInt
                   //if timestamp (in millisec) is within same time window e.g. 5 mins,
                   // assume detection algorithm able to ensure the same detection_oid
                   // if this timestamp is empty, assume detection data corrupted amd
                   // would not be used for e.g. aggregation count
                 )
