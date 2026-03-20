<?php

namespace App\Models;

use App\RiderGroup;
use App\Traits\LinkEncryptableTrait;
use Illuminate\Database\Eloquent\Factories\HasFactory;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Relations\BelongsTo;

class Rider extends Model
{
    use HasFactory, LinkEncryptableTrait;

    protected $guarded = [
        'id',
        'created_at',
        'updated_at'
    ];

    protected $encryptable = [
        'first_name',
        'last_name',
        'upn',
        'date_of_birth',
        'gender',
        'ethnicity',
        'free_school_meals',
        'send_code'
    ];

    /*
     * Decrypt Riders uses getAttribute method referenced in the LinkEncryptable Trait
     */
    public static function decryptRiders($encryptedRiders){
        // Loop through the riders and manually decrypt the first_name and last_name attributes
        $riders = $encryptedRiders->map(function ($rider) {
            $rider->first_name = $rider->getAttribute('first_name');
            $rider->last_name = $rider->getAttribute('last_name');
            $rider->upn = $rider->getAttribute('upn');
            $rider->date_of_birth = $rider->getAttribute('date_of_birth');
            $rider->gender = $rider->getAttribute('gender');
            $rider->ethnicity = $rider->getAttribute('ethnicity');
            $rider->free_school_meals = $rider->getAttribute('free_school_meals');
            $rider->send_code = $rider->getAttribute('send_code');
            return $rider;
        });
        return $riders;
    }
    // Max Riders Per Instructor Values
    const maxRidersPerInstructor = [
        [
            'course_id' => 'level_1',
            'maxRiderPerInstructor' => 12
        ],
        [
            'course_id' => 'level_2',
            'maxRiderPerInstructor' => 6
        ],
        [
            'course_id' => 'plus_learn',
            'maxRiderPerInstructor' => 3
        ],
        [
            'course_id' => 'plus_balance',
            'maxRiderPerInstructor' => 12
        ],
    ];

    // Attribute Overrides
    // Accessor for first_name

    public function getFullNameAttribute()
    {
        return "{$this->first_name} {$this->last_name}";
    }

    //Relationships
    public function courses()
    {
        return $this->belongsToMany('App\Course', 'join_riders_courses', 'rider_id')
            ->withPivot('instructor_feedback', 'attended', 'attended_override', 'has_completed_course', 'has_survey_completed', 'outcomes', 'instructors', 'withdrawn', 'withdrawal_reason');
    }

    public static function checkRiderCourseCompletion($riderId, $deliveryId){
        $rider = Rider::find($riderId);
        $attemtedCourses = $rider->courses()
              ->whereHas('delivery', function ($query) use ($deliveryId) {
                  $query->where('id', $deliveryId); // Filter by the given delivery ID
              })
              ->whereNotNull('join_riders_courses.outcomes') // Check that 'outcomes' is not null
              ->get();

        return count($attemtedCourses) > 0 ? $attemtedCourses->every(function ($course) {
            return $course->pivot->has_completed_course;
        }) : false;
    }

    public function consents()
    {
        return $this->hasMany('App\Consent','rider_id');
    }

    public function deliveries()
    {
        return $this->belongsToMany('App\Delivery', 'consents')
            ->withPivot(
                [
                    'id', 'year_group', 'consent_status',
                    'is_SEND','has_medical_condition',
                    'has_bike', 'has_helmet',
                    'pref_can_share_bike',
                    'is_telephone_consent','who_took_consent','is_on_waiting_list'
                ]
            )
            ->withTimestamps();
    }

    public function hasOutcomesForDelivery($deliveryId)
    {
        return (
            $this->courses()
              ->whereHas('delivery', function ($query) use ($deliveryId) {
                  $query->where('id', $deliveryId); // Filter by the given delivery ID
              })
              ->whereNotNull('join_riders_courses.outcomes') // Check that 'outcomes' is not null
              ->count() > 0);
    }

    public function canRemoveFromDelivery($deliveryId)
    {
        return (
            $this->courses()
                ->whereHas('delivery', function ($query) use ($deliveryId) {
                    $query->where('id', $deliveryId); // Filter by the given delivery ID
                })
                ->where(function($query) {
                    $query->whereNotNull('join_riders_courses.outcomes') // Check that 'outcomes' is not null
                    ->orWhere('has_completed_course', true);
                })
                ->count() == 0);
    }

    public static function checkRiderDuplication($firstName, $lastName, $dateOfbirth)
    {
        $inputs = [
            'hash_first_name'    => hash('sha256', strtolower($firstName)),
            'hash_last_name' => hash('sha256', strtolower($lastName)),
            'hash_date_of_birth' => hash('sha256', strtolower($dateOfbirth)),
        ];

        $rider = Rider::where([
            'hash_first_name'    => $inputs['hash_first_name'],
            'hash_last_name'     => $inputs['hash_last_name'],
            'hash_date_of_birth' => $inputs['hash_date_of_birth'],
        ])->get()->first();

        return $rider;
    }

}
