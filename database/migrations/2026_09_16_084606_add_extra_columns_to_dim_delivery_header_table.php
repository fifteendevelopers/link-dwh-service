<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    /**
     * Run the migrations.
     */
    public function up(): void
    {
        Schema::table('dim_delivery_header', function (Blueprint $table) {
            // Legacy & Alternative Grant Identifiers
            $table->string('Short_Description', 10)->nullable()->after('Delivery_Status');
            $table->unsignedInteger('Local_Funding')->default(0)->after('Delivery_Details');
            $table->string('Legacy_Delivery_Id')->nullable()->after('Local_Funding');
            $table->string('Legacy_Delivery_Method')->nullable()->after('Legacy_Delivery_Id');

            // Operational & Communication Flags
            $table->boolean('Rider_List_Uploaded')->default(false)->after('Digitisation_Booking');
            $table->string('Url_Code', 20)->nullable()->after('Rider_List_Uploaded');
            $table->boolean('Venue_Notification_Sent')->default(false)->after('Comms_Start_Date');
            $table->boolean('Survey_Notifications_Sent')->default(false)->after('Date_Completed');
            $table->date('Confirm_Booked_Email_Sent')->nullable()->after('Fleet_Cycles_Used');
            $table->date('Confirm_Booked_Reminder_Email_Sent')->nullable()->after('Confirm_Booked_Email_Sent');
            $table->boolean('Housekeeping_Selected')->default(false)->after('Waiting_List_Enabled');
            $table->boolean('Enable_School_Management')->default(false)->after('Housekeeping_Selected');

            // Source Timestamps
            $table->timestamp('Source_Created_At')->nullable()->after('Enable_School_Management');
            $table->timestamp('Source_Updated_At')->nullable()->after('Source_Created_At');
            $table->timestamp('Source_Deleted_At')->nullable()->after('Source_Updated_At');
        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::table('dim_delivery_header', function (Blueprint $table) {
            $table->dropColumn([
                'Short_Description',
                'Local_Funding',
                'Legacy_Delivery_Id',
                'Legacy_Delivery_Method',
                'Rider_List_Uploaded',
                'Url_Code',
                'Venue_Notification_Sent',
                'Survey_Notifications_Sent',
                'Confirm_Booked_Email_Sent',
                'Confirm_Booked_Reminder_Email_Sent',
                'Housekeeping_Selected',
                'Enable_School_Management',
                'Source_Created_At',
                'Source_Updated_At',
                'Source_Deleted_At',
            ]);
        });
    }
};
